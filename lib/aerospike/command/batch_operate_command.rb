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

  class BatchOperateCommand < MultiCommand #:nodoc:

    attr_accessor :batch, :policy, :attr, :records

    def initialize(node, batch, policy, records)
      super(node)
      @batch = batch
      @policy = policy
      @records = records
    end

    def batch_flags
      flags = 0
      # flags |= 0x1 if @policy.allow_inline
      flags |= 0x2 if @policy.allow_inline_ssd
      flags |= 0x4 if @policy.respond_all_keys
      flags
    end

    def write_buffer
      field_count = 1

      exp_size = estimate_expression_size(@policy.filter_exp)
      @data_offset += exp_size
      field_count += 1 if exp_size > 0

      @data_buffer.reset
      begin_cmd
      @data_offset += FIELD_HEADER_SIZE + 4 + 1 # batch.keys.length + flags

      prev = nil
      @records.each do |record|
        key = record.key
        @data_offset += key.digest.length + 4 # 4 byte batch offset

        if !@policy.send_key && !prev.nil? && prev.key.namespace == key.namespace && prev.key.set_name == key.set_name && record == prev
          @data_offset += 1
        else
          @data_offset += 12
          @data_offset += key.namespace.bytesize + FIELD_HEADER_SIZE
          @data_offset += key.set_name.bytesize + FIELD_HEADER_SIZE
          @data_offset += record.size
        end

        prev = record
      end
      size_buffer
      write_batch_header(policy, field_count)

      write_filter_exp(@policy.filter_exp, exp_size)

      field_size_offset = @data_offset

      write_field_header(0, Aerospike::FieldType::BATCH_INDEX)
      @data_offset += @data_buffer.write_int32(batch.records.length, @data_offset)
      @data_offset += @data_buffer.write_byte(batch_flags, @data_offset)

      prev = nil
      attr = BatchAttr.new
      batch.records.each_with_index do |record, index|
        @data_offset += @data_buffer.write_int32(index, @data_offset)
        key = record.key
        @data_offset += @data_buffer.write_binary(key.digest, @data_offset)

        if !@policy.send_key && !prev.nil? && prev.key.namespace == key.namespace && prev.key.set_name == key.set_name && record == prev
          @data_offset += @data_buffer.write_byte(BATCH_MSG_REPEAT, @data_offset)
        else
          case record
          when BatchRead
            attr.set_batch_read(record.policy)
            if record.bin_names&.length&.> 0
              write_batch_bin_names(key, record.bin_names, attr, attr.filter_exp)
            elsif record.ops&.length&.> 0
              attr.adjust_read(br.ops)
              write_batch_operations(key, record.ops, attr, attr.filter_exp)
            else
              attr.adjust_read_all_bins(record.read_all_bins)
              write_batch_read(key, attr, attr.filter_exp, 0)
            end

          when BatchWrite
            attr.set_batch_write(record.policy)
            attr.adjust_write(record.ops)
            write_batch_operations(key, record.ops, attr, attr.filter_exp)

          when BatchUDF
            attr.set_batch_udf(record.policy)
            write_batch_write(key, attr, attr.filter_exp, 3, 0)
            write_field_string(record.package_name, Aerospike::FieldType::UDF_PACKAGE_NAME)
            write_field_string(record.function_name, Aerospike::FieldType::UDF_FUNCTION)
            write_field_bytes(record.arg_bytes, Aerospike::FieldType::UDF_ARGLIST)

          when BatchDelete
            attr.set_batch_delete(record.policy)
            write_batch_write(key, attr, attr.filter_exp, 0, 0)
          end

          prev = record
        end
      end

      @data_buffer.write_uint32(@data_offset-MSG_TOTAL_HEADER_SIZE-4, field_size_offset)

      end_cmd
      mark_compressed(@policy)
    end

    # Parse all results in the batch.  Add records to shared list.
    # If the record was not found, the bins will be nil.
    def parse_row(result_code)
      generation = @data_buffer.read_int32(6)
      expiration = @data_buffer.read_int32(10)
      batch_index = @data_buffer.read_int32(14)
      field_count = @data_buffer.read_int16(18)
      op_count = @data_buffer.read_int16(20)

      skip_key(field_count)
      req_key = records[batch_index].key

      records[batch_index].result_code = result_code
      case result_code
      when 0, ResultCode::UDF_BAD_RESPONSE
        record = parse_record(req_key, op_count, generation, expiration)
        records[batch_index].record = record
      end
    end

  end # class

end # module
