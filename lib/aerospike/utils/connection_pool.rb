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

    attr_accessor :cluster, :host, :number_of_creations

    def initialize(cluster, host)
      self.cluster = cluster
      self.host = host
      @number_of_creations = 0
      @mutex = Mutex.new
      super(cluster.connection_queue_size)
    end

    def create
      conn = nil
      @mutex.synchronize do
        if @number_of_creations >= @max_size
          raise Aerospike::Exceptions::MaxConnectionsExceeded
        else
          conn = cluster.create_connection(host)
          if conn.connected?
            @number_of_creations += 1
          end
        end
      end
      conn
    end

    def check(conn)
      conn.alive?
    end

    def cleanup(conn)
      conn.close if conn
    end
  end
end
