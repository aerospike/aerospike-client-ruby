# frozen_string_literal: true
# Copyright 2017 Aerospike, Inc.
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

module Aerospike
  class ConnectionPool < Pool

    attr_accessor :cluster, :host, :total_connections

    # Creates a new connection pool.
    # @param cluster [Aerospike::Cluster] The Aerospike cluster that this connection pool belongs to.
    # @param host [Aerospike::Host] The host that this connection pool connects to.
    def initialize(cluster, host)
      self.cluster = cluster
      self.host = host
      @total_connections = 0
      @mutex = Mutex.new
      super(cluster.connection_queue_size)
    end

    # Creates a new connection to the Aerospike server node.
    # @return [Aerospike::Connection] A new connection to the Aerospike server node.
    # @raise [Aerospike::Exceptions::MaxConnectionsExceeded] if the maximum number of connections has been reached.
    def create
      conn = nil
      @mutex.synchronize do
        if @total_connections >= @max_size
          raise Aerospike::Exceptions::MaxConnectionsExceeded
        else
          conn = cluster.create_connection(host)
          if conn.connected?
            @total_connections += 1
          end
        end
      end
      conn
    end

    # Checks if the given connection is alive.
    # @param conn [Aerospike::Connection] The connection to check.
    # @return [Boolean] `true` if the connection is alive, `false` otherwise.
    def check(conn)
      conn.alive?
    end

    # Cleans up the given connection by closing it and decrementing the connection count.
    # @param conn [Aerospike::Connection] The connection to clean up.
    def cleanup(conn)
      @mutex.synchronize do
        begin
          if conn&.connected?
            conn.close
          end
        rescue => e
          Aerospike.logger.error("Error occurred while closing a connection")
          raise e
        end
        @total_connections -= 1
      end
    end

    def connections
      @pool.keys.map { |id| @pool[id].object }
    end

    # Closes all the connections in the pool.
    def close_all
      connections.each do |conn|
        conn.close if conn.connected?
      end
      @total_connections = 0
    end

    # Destroys the connection pool and closes all the connections.
    def self.finalize(id)
      ObjectSpace._id2ref(id).close_all
    end
  end
end
