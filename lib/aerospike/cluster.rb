# frozen_string_literal: true

# Copyright 2014-2020 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require 'set'

require 'aerospike/atomic/atomic'

module Aerospike
  class Cluster
    attr_reader :connection_timeout, :connection_queue_size, :user, :password, :features, :tls_options, :cluster_id, :aliases, :cluster_name, :client_policy
    attr_accessor :rack_aware, :rack_id, :session_token, :session_expiration

    def initialize(policy, hosts)
      @client_policy = policy
      @cluster_seeds = hosts
      @fail_if_not_connected = policy.fail_if_not_connected
      @connection_queue_size = policy.connection_queue_size
      @connection_timeout = policy.timeout
      @tend_interval = policy.tend_interval
      @cluster_name = policy.cluster_name
      @tls_options = policy.tls
      @rack_aware = policy.rack_aware
      @rack_id = policy.rack_id

      @replica_index = Atomic.new(0)

      @aliases = {}
      @cluster_nodes = []
      @partition_write_map = {}
      @node_index = Atomic.new(0)
      @features = Atomic.new(Set.new)
      @closed = Atomic.new(true)
      @mutex = Mutex.new
      @cluster_config_change_listeners = Atomic.new([])

      @old_node_count = 0

      # setup auth info for cluster
      if policy.requires_authentication
        @user = policy.user
        @password = LoginCommand.hash_password(policy.password)
      end

      initialize_tls_host_names(hosts) if tls_enabled?

      if policy.min_connections_per_node > policy.max_connections_per_node
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::PARAMETER_ERROR, "Invalid policy configuration: Minimum connections per node cannot be greater than maximum connections per node.")
      end
    end

    def connect
      wait_till_stablized

      if @fail_if_not_connected && !connected?
        raise Aerospike::Exceptions::Aerospike, Aerospike::ResultCode::SERVER_NOT_AVAILABLE
      end

      launch_tend_thread

      Aerospike.logger.info('New cluster initialized and ready to be used...')
    end

    def credentials_given?
      !(@user.nil? || @user.empty?)
    end

    def session_valid?
      @session_token && @session_expiration && @session_expiration.to_i < Time.now.to_i
    end

    def reset_session_info
      @session_token = nil
      @session_expiration = nil
    end

    def tls_enabled?
      !tls_options.nil? && tls_options[:enable] != false
    end

    def initialize_tls_host_names(hosts)
      hosts.each do |host|
        host.tls_name ||= cluster_id.nil? ? host.name : cluster_id
      end
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

    # Returns a node on the cluster for read operations
    def batch_read_node(partition, replica_policy)
      case replica_policy
      when Aerospike::Replica::MASTER, Aerospike::Replica::SEQUENCE
          master_node(partition)
      when Aerospike::Replica::MASTER_PROLES
          master_proles_node(partition)
      when Aerospike::Replica::PREFER_RACK
          rack_node(partition, seq)
      when Aerospike::Replica::RANDOM
          random_node
      else
          raise Aerospike::Exceptions::InvalidNode("invalid policy.replica value")
      end
    end

    # Returns a node on the cluster for read operations
    def read_node(partition, replica_policy, seq)
      case replica_policy
      when Aerospike::Replica::MASTER
          master_node(partition)
      when Aerospike::Replica::MASTER_PROLES
          master_proles_node(partition)
      when Aerospike::Replica::PREFER_RACK
          rack_node(partition, seq)
      when Aerospike::Replica::SEQUENCE
          sequence_node(partition, seq)
      when Aerospike::Replica::RANDOM
          random_node
      else
          raise Aerospike::Exceptions::InvalidNode("invalid policy.replica value")
      end
    end

    # Returns a node on the cluster for read operations
    def master_node(partition)
      partition_map = partitions
      replica_array = partition_map[partition.namespace]
      raise Aerospike::Exceptions::InvalidNamespace("namespace not found in the partition map") unless replica_array

      node_array = replica_array.get[0]
      raise Aerospike::Exceptions::InvalidNamespace("namespace not found in the partition map") unless node_array

      node = node_array.get[partition.partition_id]
      raise Aerospike::Exceptions::InvalidNode if !node || !node.active?

      node
    end

    # Returns a node on the cluster
    def rack_node(partition, seq)
      partition_map = partitions
      replica_array = partition_map[partition.namespace]
      raise Aerospike::Exceptions::InvalidNamespace("namespace not found in the partition map") unless replica_array

      replica_array = replica_array.get

      is_retry = seq.value > -1

      node = nil
      fallback = nil
      for i in 1..replica_array.length
        idx = (seq.update { |v| v.succ } % replica_array.size).abs
        node = replica_array[idx].get[partition.partition_id]

        next unless node

        fallback = node

        # If fallback exists, do not retry on node where command failed,
        # even if fallback is not on the same rack.
        return fallback if is_retry && fallback && i == replica_array.length

        return node if node && node.active? && node.has_rack(partition.namespace, @rack_id)
      end

      return fallback if fallback

      raise Aerospike::Exceptions::InvalidNode
    end

    # Returns a node on the cluster for read operations
    def master_proles_node(partition)
      partition_map = partitions
      replica_array = partition_map[partition.namespace]
      raise Aerospike::Exceptions::InvalidNamespace("namespace not found in the partition map") unless replica_array

      replica_array = replica_array.get

      node = nil
      for replica in replica_array
        idx = (@replica_index.update { |v| v.succ } % replica_array.size).abs
        node = replica_array[idx].get[partition.partition_id]

        return node if node && node.active?
      end

      raise Aerospike::Exceptions::InvalidNode
    end

    # Returns a random node on the cluster
    def sequence_node(partition, seq)
      partition_map = partitions
      replica_array = partition_map[partition.namespace]
      raise Aerospike::Exceptions::InvalidNamespace("namespace not found in the partition map") unless replica_array

      replica_array = replica_array.get

      node = nil
      for replica in replica_array
        idx = (seq.update { |v| v.succ } % replica_array.size).abs
        node = replica_array[idx].get[partition.partition_id]

        return node if node && node.active?
      end

      raise Aerospike::Exceptions::InvalidNode
    end

    def get_node_for_key(replica_policy, key, is_write: false)
      partition = Partition.new_by_key(key)
      if is_write
        master_node(partition)
      else
        batch_read_node(partition, replica_policy)
      end
    end

    # Returns partitions pertaining to a node
    def node_partitions(node, namespace)
      res = []

      partition_map = partitions
      replica_array = partition_map[namespace]
      raise Aerospike::Exceptions::InvalidNamespace("namespace not found in the partition map") unless replica_array

      node_array = replica_array.get[0]
      raise Aerospike::Exceptions::InvalidNamespace("namespace not found in the partition map") unless node_array


      pid = 0
      for tnode in node_array.get
        res << pid if node == tnode
        pid+=1
      end

      res
    end

    # Returns a random node on the cluster
    def random_node
      # Must copy array reference for copy on write semantics to work.
      node_array = nodes
      length = node_array.length
      i = 0
      while i < length
        # Must handle concurrency with other non-tending threads, so node_index is consistent.
        idx = (@node_index.update { |v| v.succ } % node_array.length).abs
        node = node_array[idx]

        return node if node.active?

        i = i.succ
      end
      raise Aerospike::Exceptions::InvalidNode
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

      raise Aerospike::Exceptions::InvalidNode unless node

      node
    end

    # Closes all cached connections to the cluster nodes and stops the tend thread
    def close
      return if @closed.value
      # send close signal to maintenance channel
      @closed.value = true
      @tend_thread.kill

      nodes.each(&:close)
    end

    def find_alias(aliass)
      @mutex.synchronize do
        @aliases[aliass]
      end
    end

    def update_partitions(parser)
      nmap = parser.update_partitions(partitions)
      set_partitions(nmap) if nmap
    end

    def request_info(policy, *commands)
      node = random_node
      conn = node.get_connection(policy.timeout)
      Info.request(conn, *commands).tap do
        node.put_connection(conn)
      end
    end

    def request_node_info(node, policy, *commands)
      conn = node.get_connection(policy.timeout)
      Info.request(conn, *commands).tap do
        node.put_connection(conn)
      end
    end

    def supports_feature?(feature)
      @features.get.include?(feature.to_s)
    end

    def supports_peers_protocol?
      nodes.all? { |node| node.supports_feature?(Aerospike::Features::PEERS) }
    end

    def change_password(user, password)
      # change password ONLY if the user is the same
      @password = password if @user == user
    end

    def add_cluster_config_change_listener(listener)
      @cluster_config_change_listeners.update do |listeners|
        listeners.push(listener)
      end
    end

    def remove_cluster_config_change_listener(listener)
      @cluster_config_change_listeners.update do |listeners|
        listeners.delete(listener)
      end
    end

    def inspect
      "#<Aerospike::Cluster @cluster_nodes=#{@cluster_nodes}>"
    end

    def launch_tend_thread
      @tend_thread = Thread.new do
        Thread.current.abort_on_exception = false
        loop do

            tend
            sleep(@tend_interval / 1000.0)
        rescue => e
            Aerospike.logger.error("Exception occured during tend: #{e}")
            Aerospike.logger.debug { e.backtrace.join("\n") }

        end
      end
    end

    # Check health of all nodes in cluster
    def tend
      was_changed = refresh_nodes

      return unless was_changed

      update_cluster_features
      notify_cluster_config_changed
      # only log the tend finish IF the number of nodes has been changed.
      # This prevents spamming the log on every tend interval
      log_tend_stats(nodes)
    end

    # Refresh status of all nodes in cluster. Adds new nodes and/or removes
    # unhealty ones
    def refresh_nodes
      cluster_config_changed = false

      nodes = self.nodes
      if nodes.empty?
        seed_nodes
        cluster_config_changed = true
        nodes = self.nodes
      end

      peers = Peers.new

      # Clear node reference count
      nodes.each do |node|
        node.refresh_reset
      end

      peers.use_peers = supports_peers_protocol?

      # refresh all known nodes
      nodes.each do |node|
        node.refresh_info(peers)
      end

      # refresh peers when necessary
      if peers.generation_changed?
        # Refresh peers for all nodes that responded the first time even if only
        # one node's peers changed.
        peers.reset_refresh_count!

        nodes.each do |node|
          node.refresh_peers(peers)
        end
      end

      nodes.each do |node|
        node.refresh_partitions(peers) if node.partition_generation.changed?
        node.refresh_racks if node.rebalance_generation.changed?
      end

      if peers.generation_changed? || !peers.use_peers?
        nodes_to_remove = find_nodes_to_remove(peers.refresh_count)
        if nodes_to_remove.any?
          remove_nodes(nodes_to_remove)
          cluster_config_changed = true
        end
      end

      # Add any new nodes from peer refresh
      if peers.nodes.any?
        # peers.nodes is a Hash. Pass only values, ie. the array of nodes
        add_nodes(peers.nodes.values)
        cluster_config_changed = true
      end


      cluster_config_changed
    end

    def log_tend_stats(nodes)
      diff = nodes.size - @old_node_count
      action = "#{diff.abs} #{diff.abs == 1 ? 'node has' : 'nodes have'} #{diff > 0 ? 'joined' : 'left'} the cluster."
      Aerospike.logger.info("Tend finished. #{action} Old node count: #{@old_node_count}, New node count: #{nodes.size}")
      @old_node_count = nodes.size
    end

    def wait_till_stablized
      count = -1
      done = false

      # will run until the cluster is stabilized
      thr = Thread.new do
        loop do
          tend

          # Check to see if cluster has changed since the last Tend.
          # If not, assume cluster has stabilized and return.
          break if count == nodes.length

          # Break if timed out
          break if done

          sleep(0.001) # sleep for a millisecond

          count = nodes.length
        end
      end

      # wait for the thread to finish or timeout
      # This will give the client up to 10 times the timeout duration to find
      # a host and connect successfully eventually, in case the DNS
      # returns multiple IPs and some of them are not reachable.
      thr.join(@connection_timeout * 10)
      done = true
      sleep(0.001)
      thr.kill if thr.alive?

      @closed.value = false if @cluster_nodes.length > 0
    end

    def update_cluster_features
      # Cluster supports features that are supported by all nodes
      @features.update do
        node_features = nodes.map(&:features)
        node_features.reduce(&:intersection) || Set.new
      end
    end

    def notify_cluster_config_changed
      listeners = @cluster_config_change_listeners.get
      listeners.each do |listener|
        listener.send(:cluster_config_changed, self)
      end
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
          seed_node_validator = NodeValidator.new(self, seed, @connection_timeout, @cluster_name, tls_options)
        rescue => e
          Aerospike.logger.error("Seed #{seed} failed: #{e}\n#{e.backtrace.join("\n")}")
          next
        end

        nv = nil
        # Seed host may have multiple aliases in the case of round-robin dns configurations.
        seed_node_validator.aliases.each do |aliass|
          if aliass == seed
            nv = seed_node_validator
          else
            begin
              nv = NodeValidator.new(self, aliass, @connection_timeout, @cluster_name, tls_options)
            rescue => e
              Aerospike.logger.error("Seed #{seed} failed: #{e}")
              next
            end
          end
          next if find_node_name(list, nv.name)

          node = create_node(nv)
          add_aliases(node)
          list << node
        end
      end

      add_nodes_copy(list) if list.length > 0
    end

    # Finds a node by name in a list of nodes
    def find_node_name(list, name)
      list.any? { |node| node.name == name }
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

    def create_node(nv)
      node = ::Aerospike::Node.new(self, nv)
      node.fill_connection_pool_up_to(@client_policy.min_connections_per_node)
      node
    end

    def create_connection(host)
      ::Aerospike::Cluster::CreateConnection.(self, host)
    end

    def find_nodes_to_remove(refresh_count)
      FindNodesToRemove.(self, refresh_count)
    end

    def find_node_in_partition_map(filter)
      partitions_list = partitions

      partitions_list.values.each do |replica_array|
        replica_array.get.each do |node_array|
          return true if node_array.value.any? { |node| node == filter }
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
      node.aliases.each do |aliass|
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
        node.aliases.each do |aliass|
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
      node_list.any? { |node| node == search }
    end

    def find_node_by_name(node_name)
      nodes.detect { |node| node.name == node_name }
    end
  end
end
