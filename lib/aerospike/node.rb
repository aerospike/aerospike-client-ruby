# frozen_string_literal: true

# Copyright 2014-2018 Aerospike, Inc.
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

require 'aerospike/atomic/atomic'

module Aerospike
  class Node

    attr_reader :reference_count, :responded, :name, :features, :cluster_name, :cluster, :host

    PARTITIONS = 4096
    FULL_HEALTH = 100

    # Initialize server node with connection parameters.
    def initialize(cluster, nv)
      @cluster = cluster
      @name = nv.name
      @aliases = Atomic.new(nv.aliases)
      @host = nv.host
      @use_new_info = Atomic.new(nv.use_new_info)
      @features = nv.features
      @cluster_name = nv.cluster_name

      # Assign host to first IP alias because the server identifies nodes
      # by IP address (not hostname).
      @host = nv.aliases[0]
      @health = Atomic.new(FULL_HEALTH)
      @partition_generation = Atomic.new(-1)
      @reference_count = Atomic.new(0)
      @responded = Atomic.new(false)
      @active = Atomic.new(true)

      @connections = Pool.new(@cluster.connection_queue_size)
      @connections.create_block = Proc.new do
        while conn = Connection.new(@host.name, @host.port, @cluster.connection_timeout)

          # need to authenticate
          if @cluster.user && @cluster.user != ''
            begin
              command = AdminCommand.new
              command.authenticate(conn, @cluster.user, @cluster.password)
            rescue => e
              # Socket not authenticated. Do not put back into pool.
              conn.close if conn
              raise e
            end
          end

          break if conn.connected?
        end
        conn
      end

      @connections.cleanup_block = Proc.new { |conn| conn.close if conn }
    end

    # Request current status from server node, and update node with the result
    def refresh
      friends = []

      begin
        conn = get_connection(1)
        info_map = Info.request(conn, "node", "partition-generation", "services", "cluster-name")
      rescue => e
        Aerospike.logger.error("Error during refresh for node #{self}: #{e}")
        Aerospike.logger.error(e.backtrace.join("\n"))

        conn.close if conn
        decrease_health

        return friends
      end

      verify_node_name_and_cluster_name(info_map)
      restore_health

      responded!

      friends = add_friends(info_map)
      update_partitions(conn, info_map)
      put_connection(conn)
      friends
    end

    # Get a connection to the node. If no cached connection is not available,
    # a new connection will be created
    def get_connection(timeout)
      loop do
        conn = @connections.poll
        if conn.connected?
          conn.timeout = timeout.to_f
          return conn
        end
      end
    end

    # Put back a connection to the cache. If cache is full, the connection will be
    # closed and discarded
    def put_connection(conn)
      conn.close if !active?
      @connections.offer(conn)
    end

    # Mark the node as healthy
    def restore_health
      # There can be cases where health is full, but active is false.
      # Once a node has been marked inactive, it stays inactive.
      @health.value = FULL_HEALTH
    end

    # Decrease node Health as a result of bad connection or communication
    def decrease_health
      @health.update { |v| v - 1 }
    end

    # Check if the node is unhealthy
    def unhealthy?
      @health.value <= 0
    end

    # Retrieves host for the node
    def get_host
      @host
    end

    # Sets node as active
    def active!
      @active.update { |_| true }
    end

    # Sets node as inactive
    def inactive!
      @active.update { |_| false }
    end

    # Checks if the node is active
    def active?
      @active.value
    end

    def increase_reference_count!
      @reference_count.update { |v| v + 1 }
    end

    def reset_reference_count!
      @reference_count.value = 0
    end

    def responded!
      @responded.value = true
    end

    def responded?
      @responded.value == true
    end

    def reset_responded!
      @responded.value = false
    end

    def aliases
      @aliases.value
    end

    # Marks node as inactice and closes all cached connections
    def close
      inactive!
      close_connections
    end

    def supports_feature?(feature)
      @features.include?(feature.to_s)
    end

    def ==(other)
      other && other.is_a?(Node) && (@name == other.name)
    end
    alias eql? ==

    def use_new_info?
      @use_new_info.value
    end

    def hash
      @name.hash
    end

    def inspect
      "#<Aerospike::Node: @name=#{@name}, @host=#{@host}>"
    end

    private

    def close_connections
      # drain connections and close all of them
      # non-blocking, does not call create_block when passed false
      while conn = @connections.poll(false)
        conn.close if conn
      end
    end


    def verify_node_name_and_cluster_name(info_map)
      info_name = info_map['node']

      if !info_name
        decrease_health
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::INVALID_NODE_ERROR, "Node name is empty")
      end

      if !(@name == info_name)
        # Set node to inactive immediately.
        inactive!
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::INVALID_NODE_ERROR, "Node name has changed. Old=#{@name} New= #{info_name}")
      end

      if cluster_name && cluster_name != info_map['cluster-name']
        inactive!
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::INVALID_NODE_ERROR, "Cluster name does not match. expected: #{cluster_name}, got: #{info_map['cluster-name']}")
      end
    end

    def add_friends(info_map)
      friend_string = info_map['services']
      friends = []

      return [] if friend_string.to_s.empty?

      friend_names = friend_string.split(';')

      friend_names.each do |friend|
        friend_info = friend.split(':')
        host = friend_info[0]
        port = friend_info[1].to_i
        aliass = Host.new(host, port)
        node = @cluster.find_alias(aliass)

        if node
          node.increase_reference_count!
        else
          unless friends.any? {|h| h == aliass}
            friends << aliass
          end
        end
      end

      friends
    end

    def update_partitions(conn, info_map)
      gen_string = info_map['partition-generation']

      raise Aerospike::Exceptions::Parse.new("partition-generation is empty") if gen_string.to_s.empty?

      generation = gen_string.to_i

      if @partition_generation.value != generation
        Aerospike.logger.info("Node #{name} partition generation #{generation} changed")
        @cluster.update_partitions(conn, self)
        @partition_generation.value = generation
      end
    end

  end # class Node

end # module
