# Copyright 2018 Aerospike, Inc.
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

  class BatchIndexCommand < MultiCommand #:nodoc:

    attr_accessor :batch
    attr_accessor :policy
    attr_accessor :bin_names
    attr_accessor :results
    attr_accessor :read_attr

    def initialize(node, batch, policy, bin_names, results, read_attr)
      super(node)
      @batch = batch
      @policy = policy
      @bin_names = bin_names
      @results = results
      @read_attr = read_attr
    end

    def write_buffer
      bin_name_size = 0
      operation_count = 0
      field_count = 1
      if bin_names
        bin_names.each do |bin_name|
          bin_name_size += bin_name.bytesize + OPERATION_HEADER_SIZE
        end
        operation_count = bin_names.length
      end
      begin_cmd
      @data_offset += FIELD_HEADER_SIZE + 4 + 1 # batch.keys.length + flags

      prev = nil
      batch.keys.each do |key|
        @data_offset += key.digest.length + 4 # 4 byte batch offset

        if prev != nil && prev.namespace == key.namespace
          @data_offset += 1
        else
          @data_offset += key.namespace.bytesize + FIELD_HEADER_SIZE + 1 + 1 + 2 + 2 # repeat/no-repeat flag + read_attr flags + field_count + operation_count
          @data_offset += bin_name_size
        end
      end
      size_buffer
      write_header(policy,read_attr | INFO1_BATCH, 0, 1, 0)
      write_field_header(0, Aerospike::FieldType::BATCH_INDEX)
      @data_buffer.write_int32(batch.keys.length, @data_offset)
      @data_offset += 4
      @data_buffer.write_byte(1, @data_offset)
      @data_offset += 1

      prev = nil

      batch.each_key_with_index do |key, index|
        @data_buffer.write_int32(index, @data_offset)
        @data_offset += 4
        @data_buffer.write_binary(key.digest, @data_offset)
        @data_offset += key.digest.bytesize

        if (prev != nil && prev.namespace == key.namespace)
          @data_buffer.write_byte(1, @data_offset)
          @data_offset += 1
        else
          @data_buffer.write_byte(0, @data_offset)
          @data_offset += 1
          @data_buffer.write_byte(read_attr, @data_offset)
          @data_offset += 1
          @data_buffer.write_int16(field_count, @data_offset)
          @data_offset += 2
          @data_buffer.write_int16(operation_count, @data_offset)
          @data_offset += 2
          write_field_string(key.namespace, Aerospike::FieldType::NAMESPACE)

          if bin_names
            bin_names.each do |bin_name|
              write_operation_for_bin_name(bin_name, Aerospike::Operation::READ)
            end
          end
          prev = key
        end
      end
      end_cmd
    end

    # Parse all results in the batch.  Add records to shared list.
    # If the record was not found, the bins will be nil.
    def parse_row(result_code)
      generation = @data_buffer.read_int32(6)
      expiration = @data_buffer.read_int32(10)
      batch_index = @data_buffer.read_int32(14)
      field_count = @data_buffer.read_int16(18)
      op_count = @data_buffer.read_int16(20)

      key = parse_key(field_count)

      if result_code == 0
        record = parse_record(key, op_count, generation, expiration)
        record.set_key(batch.key_for_index(batch_index))
        results[batch_index] = record
      end
    end

  end # class

end # module
