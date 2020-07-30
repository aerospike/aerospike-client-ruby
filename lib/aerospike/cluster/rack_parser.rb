# frozen_string_literal: true

# Copyright 2014-2020 Aerospike, Inc.
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

  class RackParser #:nodoc:

    attr_accessor :rebalance_generation, :racks

    REBALANCE_GENERATION = "rebalance-generation"
    RACK_IDS = "rack-ids"

    def initialize(node, conn)
      @node = node
      @conn = conn
      @racks = nil
    end

    def update_racks
      # Use low-level info methods and parse byte array directly for maximum performance.
      # Receive format: rack-ids\t
      #                 <ns1>:<rack-id>...;
      #                 <ns2>:<rack-id>...; ...
      info_map = Info.request(@conn, REBALANCE_GENERATION, RACK_IDS)

      @rebalance_generation = info_map[REBALANCE_GENERATION].to_i

      info = info_map[RACK_IDS]
      if !info || info.length == 0
        raise Aerospike::Exceptions::Connection.new("#{RACK_IDS} response for node #{@node.name} is empty")
      end

      @buffer = info
      @length = info.length
      @offset = 0

      while @offset < @length && @buffer[@offset] != '\n'
        namespace = parse_name
        rack_id = parse_rack_id

        @racks = {} if !@racks
        @racks[namespace] = rack_id
      end

      @racks
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
          "Invalid rack namespace #{namespace}. Response=#{response}"
        )
      end

      @offset+=1
      namespace
    end

    def parse_rack_id
      beginning = @offset
      while @offset < @length
        break if @buffer[@offset] == ';'
        @offset+=1
      end

      # Parse rack_id
      rack_id = @buffer[beginning...@offset].strip.to_i

      if rack_id < 0
        response = get_truncated_response
        raise Aerospike::Exceptions::Parse.new(
          "Invalid rack_id #{rack_id}. Response=#{response}"
        )
      end

      @offset+=1
      rack_id
    end

    def get_truncated_response
      max = @length
      @length = max if @length > 200
      @buffer[0...max]
    end


  end # class

end # module
