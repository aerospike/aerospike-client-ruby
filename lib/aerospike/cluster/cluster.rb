# encoding: utf-8
# Copyright 2014 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require 'thread'
require 'timeout'

require 'aerospike/atomic/atomic'

module Aerospike

  private

  class Cluster

    attr_reader :connection_timeout, :connection_queue_size, :user, :password

    def initialize(policy, *hosts)
      @cluster_seeds = hosts
      @connection_queue_size = policy.connection_queue_size
      @connection_timeout = policy.timeout
      @aliases = {}
      @cluster_nodes = []
      @partition_write_map = {}
      @node_index = Atomic.new(0)
      @closed = Atomic.new(true)
      @mutex = Mutex.new

      # setup auth info for cluster
      if policy.requires_authentication
        @user = policy.user
        @password = AdminCommand.hash_password(policy.password)
      end

      wait_till_stablized

      if policy.fail_if_not_connected && !connected?
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::SERVER_NOT_AVAILABLE)
      end

      launch_tend_thread

      Aerospike.logger.info('New cluster initialized and ready to be used...')

      self
    end

    def add_seeds(hosts)
      @mutex.synchronize do
        @cluster_seeds.concat(hosts)
      end
    end

    def seeds
      @mutex.synchronize do
        @cluster_seeds.dup
      end
    end

    def connected?
      # Must copy array reference for copy on write semantics to work.
      node_array = nodes
      (node_array.length > 0) && !@closed.value
    end

    def get_node(partition)
      # Must copy hashmap reference for copy on write semantics to work.
      nmap = partitions
      if node_array = nmap[partition.namespace]
        node = node_array.value[partition.partition_id]

        if node && node.active?
          return node
        end
      end

      return random_node
    end

    # Returns a random node on the cluster
    def random_node
      # Must copy array reference for copy on write semantics to work.
      node_array = nodes
      length = node_array.length
      i = 0
      while i < length
        # Must handle concurrency with other non-tending threads, so node_index is consistent.
        index = (@node_index.update{|v| v+1} % node_array.length).abs
        node = node_array[index]

        if node.active?
          return node
        end

        i = i.succ
      end
      raise Aerospike::Exceptions::InvalidNode.new
    end

    # Returns a list of all nodes in the cluster
    def nodes
      @mutex.synchronize do
        # Must copy array reference for copy on write semantics to work.
        @cluster_nodes.dup
      end
    end

    # Find a node by name and returns an error if not found
    def get_node_by_name(node_name)
      node = find_node_by_name(node_name)

      raise Aerospike::Exceptions::InvalidNode.new unless node

      node
    end

    # Closes all cached connections to the cluster nodes and stops the tend thread
    def close
      unless @closed.value
        # send close signal to maintenance channel
        @closed.value = true
        @tend_thread.kill

        nodes.each do |node|
          node.close
        end
      end

    end

    def find_alias(aliass)
      @mutex.synchronize do
        @aliases[aliass]
      end
    end

    def update_partitions(conn, node)
      # TODO: Cluster should not care about version of tokenizer
      # decouple clstr interface
      nmap = {}
      if node.use_new_info?
        Aerospike.logger.info("Updating partitions using new protocol...")

        tokens = PartitionTokenizerNew.new(conn)
        nmap = tokens.update_partition(partitions, node)
      else
        Aerospike.logger.info("Updating partitions using old protocol...")
        tokens = PartitionTokenizerOld.new(conn)
        nmap = tokens.update_partition(partitions, node)
      end

      # update partition write map
      set_partitions(nmap) if nmap

      Aerospike.logger.info("Partitions updated...")
    end

    def request_info(policy, *commands)
      node = random_node
      conn = node.get_connection(policy.timeout)
      Info.request(conn, *commands).tap do
        node.put_connection(conn)
      end
    end

    def change_password(user, password)
     # change password ONLY if the user is the same
     @password = password if @user == user
    end

    private

    def launch_tend_thread
      @tend_thread = Thread.new do
        abort_on_exception = false
        while true
          begin
            tend
            sleep 1 # 1 second
          rescue => e
            Aerospike.logger.error("Exception occured during tend: #{e}")
          end
        end
      end
    end

    def tend
      nodes = self.nodes

      # All node additions/deletions are performed in tend thread.
      # If active nodes don't exist, seed cluster.
      if nodes.empty?
        Aerospike.logger.info("No connections available; seeding...")
        seed_nodes

        # refresh nodes list after seeding
        nodes = self.nodes
      end

      # Refresh all known nodes.
      friend_list = []
      refresh_count = 0

      # Clear node reference counts.
      nodes.each do |node|
        node.reference_count.value = 0
        node.responded.value = false

        if node.active?
          begin
            friends = node.refresh
            refresh_count += 1
            friend_list.concat(friends) if friends
          rescue => e
            Aerospike.logger.error("Node `#{node}` refresh failed: #{e.backtrace.join("\n")}")
          end
        end
      end

      # Add nodes in a batch.
      add_list = find_nodes_to_add(friend_list)
      add_nodes(add_list) unless add_list.empty?

      # Handle nodes changes determined from refreshes.
      # Remove nodes in a batch.
      remove_list = find_nodes_to_remove(refresh_count)
      remove_nodes(remove_list) unless remove_list.empty?

      # Guard clause to prevent show tend_info because is disabled by config.
      # See documentation
      return unless Aerospike.tend_info
      Aerospike.logger.info("Tend finished. Live node count: #{nodes.length}")
    end

    def wait_till_stablized
      count = -1

      # will run until the cluster is stablized
      thr = Thread.new do
        abort_on_exception=true
        while true
          tend

          # Check to see if cluster has changed since the last Tend.
          # If not, assume cluster has stabilized and return.
          if count == nodes.length
            break
          end

          sleep(0.001) # sleep for a miliseconds

          count = nodes.length
        end
      end

      # wait for the thread to finish or timeout
      begin
        Timeout.timeout(1) do
          thr.join
        end
      rescue Timeout::Error
        thr.kill if thr.alive?
      end

      @closed.value = false if @cluster_nodes.length > 0

    end

    def set_partitions(part_map)
      @mutex.synchronize do
        @partition_write_map = part_map
      end
    end

    def partitions
      res = nil
      @mutex.synchronize do
        res = @partition_write_map
      end

      res
    end

    def seed_nodes
      seed_array = seeds

      Aerospike.logger.info("Seeding the cluster. Seeds count: #{seed_array.length}")

      list = []

      seed_array.each do |seed|
        begin
          seed_node_validator = NodeValidator.new(self, seed, @connection_timeout)
        rescue => e
          Aerospike.logger.error("Seed #{seed.to_s} failed: #{e.backtrace.join("\n")}")
          next
        end

        nv = nil
        # Seed host may have multiple aliases in the case of round-robin dns configurations.
        seed_node_validator.aliases.each do |aliass|

          if aliass == seed
            nv = seed_node_validator
          else
            begin
              nv = NodeValidator.new(self, aliass, @connection_timeout)
            rescue Exection => e
              Aerospike.logger.error("Seed #{seed.to_s} failed: #{e}")
              next
            end
          end

          if !find_node_name(list, nv.name)
            node = create_node(nv)
            add_aliases(node)
            list << node
          end
        end

      end

      if list.length > 0
        add_nodes_copy(list)
      end

    end

    # Finds a node by name in a list of nodes
    def find_node_name(list, name)
      list.any?{|node| node.name == name}
    end

    def add_alias(host, node)
      if host && node
        @mutex.synchronize do
          @aliases[host] = node
        end
      end
    end

    def remove_alias(aliass)
      if aliass
        @mutex.synchronize do
          @aliases.delete(aliass)
        end
      end
    end

    def find_nodes_to_add(hosts)
      list = []

      hosts.each do |host|
        begin
          nv = NodeValidator.new(self, host, @connection_timeout)

          # if node is already in cluster's node list,
          # or already included in the list to be added, we should skip it
          node = find_node_by_name(nv.name)
          node ||= list.detect{|n| n.name == nv.name}

          # make sure node is not already in the list to add
          if node
            # Duplicate node name found.  This usually occurs when the server
            # services list contains both internal and external IP addresses
            # for the same node.  Add new host to list of alias filters
            # and do not add new node.
            node.reference_count.update{|v| v + 1}
            node.add_alias(host)
            add_alias(host, node)
            next
          end

          node = create_node(nv)
          list << node

        rescue => e
          Aerospike.logger.error("Add node #{node.to_s} failed: #{e}")
        end
      end

      list
    end

    def create_node(nv)
      Node.new(self, nv)
    end

    def find_nodes_to_remove(refresh_count)
      node_list = nodes

      remove_list = []

      node_list.each do |node|
        if !node.active?
          # Inactive nodes must be removed.
          remove_list << node
          next
        end

        case node_list.length
        when 1
          # Single node clusters rely solely on node health.
          remove_list << node if node.unhealthy?

        when 2
          # Two node clusters require at least one successful refresh before removing.
          if refresh_count == 2 && node.reference_count.value == 0 && !node.responded.value
            # Node is not referenced nor did it respond.
            remove_list << node
          end

        else
          # Multi-node clusters require two successful node refreshes before removing.
          if refresh_count >= 2 && node.reference_count.value == 0
            # Node is not referenced by other nodes.
            # Check if node responded to info request.
            if node.responded.value
              # Node is alive, but not referenced by other nodes.  Check if mapped.
              if !find_node_in_partition_map(node)
                # Node doesn't have any partitions mapped to it.
                # There is not point in keeping it in the cluster.
                remove_list << node
              end
            else
              # Node not responding. Remove it.
              remove_list << node
            end
          end
        end
      end

      remove_list
    end

    def find_node_in_partition_map(filter)
      partitions_list = partitions

      partitions_list.each do |node_array|
        max = node_array.length

        i = 0
        while i < max
          node = node_array[i]
          # Use reference equality for performance.
          if node == filter
            return true
          end

          i = i.succ
        end
      end
      false
    end

    def add_nodes(nodes_to_add)
      # Add all nodes at once to avoid copying entire array multiple times.
      nodes_to_add.each do |node|
        add_aliases(node)
      end

      add_nodes_copy(nodes_to_add)
    end

    def add_aliases(node)
      # Add node's aliases to global alias set.
      # Aliases are only used in tend thread, so synchronization is not necessary.
      node.get_aliases.each do |aliass|
        @aliases[aliass] = node
      end
    end

    def add_nodes_copy(nodes_to_add)
      @mutex.synchronize do
        @cluster_nodes.concat(nodes_to_add)
      end
    end

    def remove_nodes(nodes_to_remove)
      # There is no need to delete nodes from partition_write_map because the nodes
      # have already been set to inactive. Further connection requests will result
      # in an exception and a different node will be tried.

      # Cleanup node resources.
      nodes_to_remove.each do |node|
        # Remove node's aliases from cluster alias set.
        # Aliases are only used in tend thread, so synchronization is not necessary.
        node.get_aliases.each do |aliass|
          Aerospike.logger.debug("Removing alias #{aliass}")
          remove_alias(aliass)
        end

        node.close
      end

      # Remove all nodes at once to avoid copying entire array multiple times.
      remove_nodes_copy(nodes_to_remove)
    end

    def set_nodes(nodes)
      @mutex.synchronize do
        # Replace nodes with copy.
        @cluster_nodes = nodes
      end
    end

    def remove_nodes_copy(nodes_to_remove)
      # Create temporary nodes array.
      # Since nodes are only marked for deletion using node references in the nodes array,
      # and the tend thread is the only thread modifying nodes, we are guaranteed that nodes
      # in nodes_to_remove exist.  Therefore, we know the final array size.
      nodes_list = nodes
      node_array = []
      count = 0

      # Add nodes that are not in remove list.
      nodes_list.each do |node|
        if node_exists(node, nodes_to_remove)
          Aerospike.logger.info("Removed node `#{node}`")
        else
          node_array[count] = node
          count += 1
        end
      end

      # Do sanity check to make sure assumptions are correct.
      if count < node_array.length
        Aerospike.logger.warn("Node remove mismatch. Expected #{node_array.length}, Received #{count}")

        # Resize array.
        node_array = node_array.dup[0..count-1]
      end

      set_nodes(node_array)
    end

    def node_exists(search, node_list)
      node_list.any? {|node| node == search }
    end

    def find_node_by_name(node_name)
      nodes.detect{|node| node.name == node_name }
    end

  end

end
