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

require 'aerospike/command/command'

module Aerospike

  private

  class SingleCommand < Command #:nodoc:

    def initialize(cluster, key)
      @cluster = cluster
      @key = key

      @partition = Partition.new_by_key(key)

      super()
    end

    protected

    def get_node
      # abstract method
      raise Aerospike::Exceptions::InvalidNode
    end

    def empty_socket
      # There should not be any more bytes.
      # Empty the socket to be safe.
      sz = @data_buffer.read_int64( 0)
      header_length = @data_buffer.read(8).ord
      receive_size = Integer(sz&0xFFFFFFFFFFFF) - header_length

      # Read remaining message bytes.
      if receive_size > 0
        size_buffer_sz(receive_size)
        begin
          @conn.read(@data_buffer, receive_size)
        rescue => e
          Aerospike.logger.error(e)
          raise e
        end
      end
    end

  end # class

end # module
