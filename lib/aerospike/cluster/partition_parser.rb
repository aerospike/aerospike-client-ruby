# frozen_string_literal: true

# Copyright 2014-2019 Aerospike, Inc.
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

require 'base64'

module Aerospike

  class PartitionParser #:nodoc:

    attr_accessor :copied, :partition_generation

    PARTITION_GENERATION = "partition-generation";
    REPLICAS_ALL = "replicas-all";

    def initialize(node, conn)
      @node = node
      @conn = conn
    end

    def update_partitions(current_map)
      # Use low-level info methods and parse byte array directly for maximum performance.
      # Receive format: replicas-all\t
      #                 <ns1>:<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;
      #                 <ns2>:<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;\n
      info_map = Info.request(@conn, PARTITION_GENERATION, REPLICAS_ALL)

      @partition_generation = info_map[PARTITION_GENERATION].to_i

      info = info_map[REPLICAS_ALL]
      if !info || info.length == 0
        raise Aerospike::Exceptions::Connection.new("#{REPLICAS_ALL} response for node #{@node.name} is empty")
      end

      @buffer = info
      @length = info.length
      @offset = 0

      new_map = nil
      copied = false
      beginning = @offset

      while @offset < @length && @buffer[@offset] != '\n'
        namespace = parse_name
        replica_count = parse_replica_count

        replica_array = current_map[namespace]
        if !replica_array
          if !copied
            # Make shallow copy of map.
            new_map = current_map.clone
            copied = true
          end

          replica_array = Atomic.new(Array.new(replica_count))
          new_map[namespace] = replica_array
        end

        for replica in 0...replica_count do
          node_array = (replica_array.get)[replica]

          if !node_array
            if !copied
              # Make shallow copy of map.
              new_map = current_map.clone
              copied = true
            end

            node_array = Atomic.new(Array.new(Aerospike::Node::PARTITIONS))
            new_map[namespace].update{|v| v[replica] = node_array; v}
          end

          restore_buffer = parse_bitmap
          i = 0
          while i < Aerospike::Node::PARTITIONS
            if (restore_buffer[i>>3].ord & (0x80 >> (i & 7))) != 0
              node_array.update{|v| v[i] = @node; v}
            end
            i = i.succ
          end
        end
      end

      copied ? new_map : nil
    end

    private

    def parse_name
      beginning = @offset
      while @offset < @length
        break if @buffer[@offset] == ':'
        @offset+=1
      end

      # Parse namespace.
      namespace = @buffer[beginning...@offset].strip

      if namespace.length <= 0 || namespace.length >= 32
        response = get_truncated_response
        raise Aerospike::Exceptions::Parse.new(
          "Invalid partition namespace #{namespace}. Response=#{response}"
        )
      end

      @offset+=1
      namespace
    end

    def parse_replica_count
      beginning = @offset
      while @offset < @length
        break if @buffer[@offset] == ','
        @offset+=1
      end

      # Parse count
      count = @buffer[beginning...@offset].strip.to_i

      if count < 0 || count > 4096
        response = get_truncated_response
        raise Aerospike::Exceptions::Parse.new(
          "Invalid partition count #{count}. Response=#{response}"
        )
      end

      @offset+=1
      count
    end

    def parse_bitmap
      beginning = @offset
      while @offset < @length
        break if @buffer[@offset] == ','
        break if @buffer[@offset] == ';'
        @offset+=1
      end

      bit_map_length = @offset - beginning
      restore_buffer = Base64.strict_decode64(@buffer[beginning, bit_map_length])

      @offset+=1
      restore_buffer
    end


    def get_truncated_response
      max = @length
      @length = max if @length > 200
      @buffer[0...max]
    end


  end # class

end # module
