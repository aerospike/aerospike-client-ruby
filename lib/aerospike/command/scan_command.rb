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

require 'aerospike/command/batch_command'

module Aerospike

  private

  class ScanCommand < BatchCommand

    def initialize(node, policy, namespace, set_name, bin_names, callback)
      super(node)

      @policy = policy
      @namespace = namespace
      @set_name = set_name
      @bin_names = bin_names
      @callback = callback
    end

    def write_buffer
      set_scan(@policy, @namespace, @set_name, @bin_names)
    end

    def parse_record_results(receive_size)
      @data_offset = 0

      while @data_offset < receive_size
        read_bytes(MSG_REMAINING_HEADER_SIZE)
        result_code = @data_buffer.read(5).ord & 0xFF

        # The only valid server return codes are "ok" and "not found".
        # If other return codes are received, then abort the batch.
        if result_code != 0 && result_code != Aerospike::ResultCode::KEY_NOT_FOUND_ERROR
          raise Aerospike::Exceptions::Aerospike.new(result_code)
        end

        info3 = @data_buffer.read(3).ord

        # If cmd is the end marker of the response, do not proceed further
        return false if (info3 & INFO3_LAST) == INFO3_LAST

        generation = @data_buffer.read_int32(6).ord
        expiration = @data_buffer.read_int32(10).ord
        field_count = @data_buffer.read_int16(18).ord
        op_count = @data_buffer.read_int16(20).ord
        key = parse_key(field_count)
  
        if result_code == 0
          @callback.call(parse_record(key, op_count, generation, expiration))
        end
      end # while

      true
    end

  end # class

end # module
