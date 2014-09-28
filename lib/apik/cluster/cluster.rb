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

require 'atomic'

require 'apik/cluster/node_validator'
require 'apik/cluster/node'

require 'apik/cluster/partition_tokenizer_new'
require 'apik/cluster/partition_tokenizer_old'

module Apik

  class Cluster

    attr_reader :connection_timeout, :connection_queue_size

    def initialize(policy, *hosts)
      @seeds = hosts
      @connection_queue_size = policy.ConnectionQueueSize
      @connection_timeout = policy.Timeout
      @aliases = {}
      @nodes = []
      @partition_write_map = {}
      @node_index = Atomic.new(0)
      @closed = Atomic.new(false)
      @mutex = Mutex.new

      wait_till_stablized

      if policy.FailIfNotConnected && !connected?
        raise Apik::Exceptions::Aerospike.new(Apik::ResultCode::SERVER_NOT_AVAILABLE)
      end

      launch_cluster_boss

      Apik.logger.debug('New cluster initialized and ready to be used...')

      self
    end

    def add_seeds(hosts)
      @mutex.synchronize do
        @seeds << hosts
      end
    end

    def get_seeds
      res = []
      @mutex.synchronize do
        res = @seeds.dup
      end

      res
    end

    def connected?
      # Must copy array reference for copy on write semantics to work.
      node_array = get_nodes
      (node_array.length > 0) && !@closed.value
    end

    def get_node(partition)
      # Must copy hashmap reference for copy on write semantics to work.
      nmap = get_partitions
      if node_array = nmap[partition.namespace]
        node = node_array.value[partition.partition_id]

        if node && node.active?
          return node
        end
      end

      return get_random_node
    end

    # Returns a random node on the cluster
    def get_random_node
      # Must copy array reference for copy on write semantics to work.
      node_array = get_nodes
      length = node_array.length
      for i in 0..length
        # Must handle concurrency with other non-tending goroutines, so node_index is consistent.
        index = (@node_index.update{|v| v+1} % node_array.length).abs
        node = node_array[index]

        if node.active?
          # Logger.Debug("Node `%s` is active. index=%d", node, index)
          return node
        end
      end
      raise Apik::Exceptions::InvalidNode.new
    end

    # Returns a list of all nodes in the cluster
    def get_nodes
      node_array = nil
      @mutex.synchronize do
        # Must copy array reference for copy on write semantics to work.
        node_array = @nodes.dup
      end

      node_array
    end

    # Find a node by name and returns an error if not found
    def get_node_by_name(node_name)
      node = find_node_by_name(node_name)

      raise Apik::Exceptions::InvalidNode.new unless node

      node
    end

    # Closes all cached connections to the cluster nodes and stops the tend goroutine
    def close
      unless @closed.value
        # send close signal to maintenance channel
        @closed.value = true
        @tend_thread.kill

        get_nodes.each do |node|
          node.close
        end
      end

    end

    def find_alias(aliass)
      res = nil
      @mutex.synchronize do
        res = @aliases[aliass]
      end
      res
    end

    def update_partitions(conn, node)
      # TODO: Cluster should not care about version of tokenizer
      # decouple clstr interface
      nmap = {}
      if node.use_new_info?
        Apik.logger.info("Updating partitions using new protocol...")

        tokens = PartitionTokenizerNew.new(conn)
        nmap = tokens.update_partition(get_partitions, node)
      else
        Apik.logger.info("Updating partitions using old protocol...")
        tokens = PartitionTokenizerOld.new(conn)
        nmap = tokens.update_partition(get_partitions, node)
      end

      # update partition write map
      if nmap
        set_partitions(nmap)
      end

      Apik.logger.info("Partitions updated...")
    end

    private

    def launch_cluster_boss

      @tend_thread = Thread.new do

        while true
          begin
            tend
            sleep 1 # 1 second
          rescue Exception => e
            Apik.logger.error(e)
          end
        end
      end

    end

    def tend
      nodes = get_nodes

      # All node additions/deletions are performed in tend goroutine.
      # If active nodes don't exist, seed cluster.
      if nodes.length == 0
        Apik.logger.info("No connections available; seeding...")
        seed_nodes

        # refresh nodes list after seeding
        nodes = get_nodes
      end

      # Clear node reference counts.
      nodes.each do |node|
        node.reference_count.value = 0
        node.responded.value = false
      end

      # Refresh all known nodes.
      friend_list = []
      refresh_count = 0

      nodes.each do |node|
        if node.active?
          begin
            friends = node.refresh
            refresh_count += 1
            if friends
              friend_list.concat(friends)
            end
          rescue Exception => e
            Apik.logger.warn("Node `#{node}` refresh failed: #{e.to_s}")
          end
        end
      end

      # Handle nodes changes determined from refreshes.
      # Remove nodes in a batch.
      remove_list = find_nodes_to_remove(refresh_count)
      if remove_list.length > 0
        remove_nodes(remove_list)
      end

      # Add nodes in a batch.
      add_list = find_nodes_to_add(friend_list)
      if add_list.length > 0
        add_nodes(add_list)
      end

      # get_nodes.each do |n|
      #   p n.name
      # end

      Apik.logger.info("Tend finished. Live node count: #{get_nodes.length}")
    end

    def wait_till_stablized
      count = -1

      # will run until the cluster is stablized
      thr = Thread.new do
        while true
          tend

          # Check to see if cluster has changed since the last Tend.
          # If not, assume cluster has stabilized and return.
          if count == get_nodes.length
            break
          end

          sleep(0.001) # sleep for a miliseconds

          count = get_nodes.length
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

    end

    def set_partitions(part_map)
      @mutex.synchronize do
        @partition_write_map = part_map
      end
    end

    def get_partitions
      res = nil
      @mutex.synchronize do
        res = @partition_write_map
      end

      res
    end

    def seed_nodes
      seed_array = get_seeds

      Apik.logger.info("Seeding the cluster. Seeds count: #{seed_array.length}")

      list = []

      seed_array.each do |seed|
        begin
          seed_node_validator = NodeValidator.new(seed, @connection_timeout)
        rescue Exception => e
          Apik.logger.warn("Seed #{seed.to_s} failed: #{e}")
          next
        end

        nv = nil
        # Seed host may have multiple aliases in the case of round-robin dns configurations.
        seed_node_validator.aliases.each do |aliass|

          if aliass == seed
            nv = seed_node_validator
          else
            begin
              nv = NodeValidator.new(aliass, @connection_timeout)
            rescue Exection => e
              Apik.logger.warn("Seed #{seed.to_s} failed: #{e}")
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
      list.any?{|name| node.name == name}
    end

    def add_alias(host, node)
      if host && node
        @mutex.synchronize do
          aliases[host] = node
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
          nv = NodeValidator.new(host, @connection_timeout)

          node = find_node_by_name(nv.name)

          # make sure node is not already in the list to add
          if node
            list.each do |n|
              if n.name == nv.name
                node = n
                break
              end
            end
          end

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

        rescue Exception => e
          Apik.logger.warn("Add node #{node.to_s} failed: #{e}")
        end
      end

      list
    end

    def create_node(nv)
      Node.new(self, nv)
    end

    def find_nodes_to_remove(refresh_count)
      nodes = get_nodes

      remove_list = []

      nodes.each do |node|
        if !node.active?
          # Inactive nodes must be removed.
          remove_list << node
          next
        end

        case nodes.length
        when 1
          # Single node clusters rely solely on node health.
          remove_list << node if node.unhealthy?

        when 2
          # Two node clusters require at least one successful refresh before removing.
          if refresh_count == 1 && node.reference_count.value == 0 && !node.responded.value
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
      partitions = get_partitions

      partitions.each do |node_array|
        max = node_array.length

        for i in 0...max
          node = node_array[i]
          # Use reference equality for performance.
          if node == filter
            return true
          end
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
      # Aliases are only used in tend goroutine, so synchronization is not necessary.
      node.get_aliases.each do |aliass|
        @aliases[aliass] = node
      end
    end

    def add_nodes_copy(nodes_to_add)
      @mutex.synchronize do
        @nodes.concat(nodes_to_add)
      end
    end

    def remove_nodes(nodes_to_remove)
      # There is no need to delete nodes from partition_write_map because the nodes
      # have already been set to inactive. Further connection requests will result
      # in an exception and a different node will be tried.

      # Cleanup node resources.
      nodes_to_remove.each do |node|
        # Remove node's aliases from cluster alias set.
        # Aliases are only used in tend goroutine, so synchronization is not necessary.
        node.get_aliases.each do |aliass|
          Apik.logger.debug("Removing alias #{aliass}")
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
        @nodes = nodes
      end
    end

    def remove_nodes_copy(nodes_to_remove)
      # Create temporary nodes array.
      # Since nodes are only marked for deletion using node references in the nodes array,
      # and the tend goroutine is the only goroutine modifying nodes, we are guaranteed that nodes
      # in nodes_to_remove exist.  Therefore, we know the final array size.
      nodes = get_nodes
      node_array = []
      count = 0

      # Add nodes that are not in remove list.
      nodes.each do |node|
        if node_exists(node, nodes_to_remove)
          Apik.logger.info("Removed node `#{node}`")
        else
          node_array[count] = node
          count += 1
        end
      end

      # Do sanity check to make sure assumptions are correct.
      if count < node_array.length
        Apik.logger.warn("Node remove mismatch. Expected #{node_array.length}, Received #{count}")

        # Resize array.
        node_array = node_array.dup[0..count-1]
      end

      set_nodes(node_array)
    end

    def node_exists(search, node_list)
      node_list.any? {|node| node.equals(search) }
    end

    def find_node_by_name(node_name)
      # Must copy array reference for copy on write semantics to work.
      get_nodes.detect{|node| node.name == node_name }
    end

  end

end
