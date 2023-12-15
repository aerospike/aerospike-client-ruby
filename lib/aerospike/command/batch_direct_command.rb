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

require 'aerospike/command/multi_command'

module Aerospike

  class BatchDirectCommand < MultiCommand #:nodoc:

    attr_accessor :batch
    attr_accessor :policy
    attr_accessor :key_map
    attr_accessor :bin_names
    attr_accessor :results
    attr_accessor :read_attr

    def initialize(node, batch, policy, key_map, bin_names, results, read_attr)
      super(node)

      @batch = batch
      @policy = policy
      @key_map = key_map
      @bin_names = bin_names
      @results = results
      @read_attr = read_attr
    end

    def write_buffer
      # Estimate buffer size
      begin_cmd
      byte_size = batch.keys.length * DIGEST_SIZE

      @data_offset += batch.namespace.bytesize +
        FIELD_HEADER_SIZE + byte_size + FIELD_HEADER_SIZE

      if bin_names
        bin_names.each do |bin_name|
          estimate_operation_size_for_bin_name(bin_name)
        end
      end

      size_buffer

      operation_count = 0
      if bin_names
        operation_count = bin_names.length
      end

      write_header_read(policy, read_attr, 0, 2, operation_count)
      write_field_string(batch.namespace, Aerospike::FieldType::NAMESPACE)
      write_field_header(byte_size, Aerospike::FieldType::DIGEST_RIPE_ARRAY)

      batch.keys.each do |key|
        @data_offset += @data_buffer.write_binary(key.digest, @data_offset)
      end

      if bin_names
        bin_names.each do |bin_name|
          write_operation_for_bin_name(bin_name, Aerospike::Operation::READ)
        end
      end

      end_cmd
      mark_compressed(@policy)
    end

    # Parse all results in the batch.  Add records to shared list.
    # If the record was not found, the bins will be nil.
    def parse_row(result_code)
      generation = @data_buffer.read_int32(6)
      expiration = @data_buffer.read_int32(10)
      field_count = @data_buffer.read_int16(18)
      op_count = @data_buffer.read_int16(20)

      key = parse_key(field_count)

      item = key_map[key.digest]
      if item
        if result_code == 0
          index = item.index
          key = item.key
          results[index] = parse_record(key, op_count, generation, expiration)
        end
      else
        Aerospike.logger.warn("Unexpected batch key returned: #{key}")
      end
    end

  end # class

end # module
