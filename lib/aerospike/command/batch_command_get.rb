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

  class BatchCommandGet < BatchCommand #:nodoc:

    def initialize(node, batch_namespace, policy, key_map, bin_names, records, read_attr)
      super(node)

      @batch_namespace = batch_namespace
      @policy = policy
      @key_map = key_map
      @bin_names = bin_names
      @records = records
      @read_attr = read_attr
    end

    def write_buffer
      set_batch_get(@policy, @batch_namespace, @bin_names, @read_attr)
    end

    # Parse all results in the batch.  Add records to shared list.
    # If the record was not found, the bins will be nil.
    def parse_record_results(receive_size)
      #Parse each message response and add it to the result array
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

        generation = @data_buffer.read_int32(6)
        expiration = Aerospike.TTL(@data_buffer.read_int32(10))
        field_count = @data_buffer.read_int16(18)
        op_count = @data_buffer.read_int16(20)
        key = parse_key(field_count)
        item = @key_map[key.digest]

        if item
          if result_code == 0
            index = item.index
            @records[index] = parse_record(key, op_count, generation, expiration)
          end
        else
          Aerospike.logger.debug("Unexpected batch key returned: #{key.namespace}, #{key.digest}")
        end

      end # while

      true
    end

  end # class

end # module
