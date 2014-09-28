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

require 'base64'

module Aerospike

  protected

  REPLICAS_NAME = 'replicas-master'

  class PartitionTokenizerNew

    def initialize(conn)
      # Use low-level info methods and parse byte array directly for maximum performance.
      # Send format:    replicas-master\n
      # Receive format: replicas-master\t<ns1>:<base 64 encoded bitmap>;<ns2>:<base 64 encoded bitmap>... \n
      info_map = Info.request(conn, REPLICAS_NAME)

      info = info_map[REPLICAS_NAME]

      @length = info ? info.length : 0

      if !info || @length == 0
        raise Aerospike::Exceptions::Connection.new("#{replicas_name} is empty")
      end

      @buffer = info
      @offset = 0

      self
    end

    def update_partition(nmap, node)
      amap = nil

      beginning = @offset
      copied = false

      while @offset < @length
        if @buffer[@offset] == ':'
          # Parse namespace.
          namespace = @buffer[beginning...@offset].strip!

          if namespace.length <= 0 || namespace.length >= 32
            response = get_truncated_response
            raise Aerospike::Exceptions::Parse.new(
              "Invalid partition namespace #{namespace}. Response=#{response}"
            )
          end

          @offset+=1
          beginning = @offset

          # Parse partition id.
          while @offset < @length
            b = @buffer[@offset]

            break if b == ";" || b == "\n"
            @offset+=1
          end

          if @offset == beginning
            response = get_truncated_response

            raise Aerospike::Exceptions::Parse.new(
              "Empty partition id for namespace #{namespace}. Response=#{response}"
            )
          end

          if !nmap[namespace]
            if !copied
              # Make shallow copy of map.
              amap = {}
              nmap.each {|k, v| amap[k] = Atomic.new(v)}
              copied = true
            end

            # p "WE WERE HERE!"
            node_array = Atomic.new(Array.new(Aerospike::Node::PARTITIONS))
            amap[namespace] = node_array
          end

          bit_map_length = @offset - beginning
          restore_buffer = Base64.strict_decode64(@buffer[beginning, bit_map_length])

          for i in 0...Aerospike::Node::PARTITIONS
            if (restore_buffer[i>>3].ord & (0x80 >> (i & 7))) != 0
              # Logger.Info("Map: `" + namespace + "`," + strconv.Itoa(i) + "," + node.String)
              node_array.update{|v| v[i] = node; v}
            end
          end

          @offset+=1
          beginning = @offset
        else
          @offset+=1
        end
      end

      copied ? amap : nil
    end

    private

    def get_truncated_response
      max = @length
      @length = max if @length > 200
      @buffer[0...max]
    end


  end # class

end # module
