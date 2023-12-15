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

require 'aerospike/atomic/atomic'

module Aerospike
  class Node

    attr_reader :reference_count, :responded, :name, :features, :cluster_name, :partition_generation, :rebalance_generation, :peers_generation, :failures, :cluster, :peers_count, :host, :connections

    PARTITIONS = 4096
    FULL_HEALTH = 100
    HAS_PARTITION_SCAN = 1 << 0
    HAS_QUERY_SHOW = 1 << 1
    HAS_BATCH_ANY = 1 << 2
    HAS_PARTITION_QUERY = 1 << 3


    # Initialize server node with connection parameters.
    def initialize(cluster, nv)
      @cluster = cluster
      @name = nv.name
      @aliases = Atomic.new(nv.aliases)
      @host = nv.host
      @features = nv.features
      @cluster_name = nv.cluster_name

      # TODO: Re-use connection from node validator
      @tend_connection = nil

      # Assign host to first IP alias because the server identifies nodes
      # by IP address (not hostname).
      @host = nv.aliases[0]
      @health = Atomic.new(FULL_HEALTH)
      @peers_count = Atomic.new(0)
      @peers_generation = ::Aerospike::Node::Generation.new
      @partition_generation = ::Aerospike::Node::Generation.new
      @rebalance_generation = ::Aerospike::Node::Rebalance.new
      @reference_count = Atomic.new(0)
      @responded = Atomic.new(false)
      @active = Atomic.new(true)
      @failures = Atomic.new(0)

      @replica_index = Atomic.new(0)
      @racks = Atomic.new(nil)

      @connections = ::Aerospike::ConnectionPool.new(cluster, host)
    end

    def partition_query?
      (@features & HAS_PARTITION_QUERY) != 0
    end

    def query_show?
      (@features & HAS_QUERY_SHOW) != 0
    end

    def update_racks(parser)
      new_racks = parser.update_racks
      @racks.value = new_racks if new_racks
    end

    def has_rack(ns, rack_id)
      racks = @racks.value
      return false if !racks
      racks[ns] == rack_id
    end

    def fill_connection_pool_up_to(min_connection_size)
      current_number_of_connections = @connections.length
      if min_connection_size > 0
        while current_number_of_connections < min_connection_size
          conn = @connections.create
          @connections.offer(conn)
          current_number_of_connections += 1
        end
      end
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

    # Separate connection for refreshing
    def tend_connection
      if @tend_connection.nil? || @tend_connection.closed?
        @tend_connection = Cluster::CreateConnection.(cluster, host)
      end
      @tend_connection
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

    def referenced?
      @reference_count.value > 0
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

    def has_peers?
      @peers_count.value > 0
    end

    def failed?(threshold = 1)
      @failures.value >= threshold
    end

    def failed!
      @failures.update { |v| v + 1 }
    end

    def reset_failures!
      @failures.value = 0
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

    def hash
      @name.hash
    end

    def inspect
      "#<Aerospike::Node: @name=#{@name}, @host=#{@host}>"
    end

    ##
    # Convenience wrappers for applying refresh operations to a node
    ##

    def refresh_info(peers)
      Node::Refresh::Info.(self, peers)
    end

    def refresh_partitions(peers)
      Node::Refresh::Partitions.(self, peers)
    end

    def refresh_racks()
      Node::Refresh::Racks.(self)
    end

    def refresh_peers(peers)
      Node::Refresh::Peers.(self, peers)
    end

    def refresh_reset
      Node::Refresh::Reset.(self)
    end

    private

    def close_connections
      @tend_connection.close if @tend_connection
      # drain connections and close all of them
      # non-blocking, does not call create_block when passed false
      while conn = @connections.poll(false)
        conn.close if conn
      end
    end
  end # class Node
end # module
