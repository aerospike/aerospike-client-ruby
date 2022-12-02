# encoding: utf-8
# Copyright 2014-2020 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License") you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require "aerospike/query/stream_command"
require "aerospike/query/recordset"

module Aerospike
  private

  class QueryPartitionCommand < QueryCommand #:nodoc:
    def initialize(node, tracker, policy, statement, recordset, node_partitions)
      super(node, policy, statement, recordset, @node_partitions)
      @node_partitions = node_partitions
      @tracker = tracker
    end

    def write_buffer
      function_arg_buffer = nil
      field_count = 0
      filter_size = 0
      bin_name_size = 0

      begin_cmd

      if @statement.namespace
        @data_offset += @statement.namespace.bytesize + FIELD_HEADER_SIZE
        field_count += 1
      end

      if @statement.set_name
        @data_offset += @statement.set_name.bytesize + FIELD_HEADER_SIZE
        field_count += 1
      end

      # Estimate recordsPerSecond field size. This field is used in new servers and not used
      # (but harmless to add) in old servers.
      if @policy.records_per_second > 0
        @data_offset += 4 + FIELD_HEADER_SIZE
        field_count += 1
      end

      # Estimate socket timeout field size. This field is used in new servers and not used
      # (but harmless to add) in old servers.
      @data_offset += 4 + FIELD_HEADER_SIZE
      field_count += 1

      # Estimate task_id field.
      @data_offset += 8 + FIELD_HEADER_SIZE
      field_count += 1

      filter = @statement.filters[0]
      bin_names = @statement.bin_names
      packed_ctx = nil

      if filter
        col_type = filter.collection_type

        # Estimate INDEX_TYPE field.
        if col_type > 0
          @data_offset += FIELD_HEADER_SIZE + 1
          field_count += 1
        end

        # Estimate INDEX_RANGE field.
        @data_offset += FIELD_HEADER_SIZE
        filter_size += 1  # num filters
        filter_size += filter.estimate_size

        @data_offset += filter_size
        field_count += 1

        packed_ctx = filter.packed_ctx
        if packed_ctx
          @data_offset += FIELD_HEADER_SIZE + packed_ctx.length
          field_count += 1
        end
      end

      @statement.set_task_id
      predexp = @policy.predexp || @statement.predexp

      if predexp
        @data_offset += FIELD_HEADER_SIZE
        pred_size = Aerospike::PredExp.estimate_size(predexp)
        @data_offset += pred_size
        field_count += 1
      end

      unless @policy.filter_exp.nil?
        exp_size = estimate_expression_size(@policy.filter_exp)
        field_count += 1 if exp_size > 0
      end

      # Estimate aggregation/background function size.
      if @statement.function_name
        @data_offset += FIELD_HEADER_SIZE + 1 # udf type
        @data_offset += @statement.package_name.bytesize + FIELD_HEADER_SIZE
        @data_offset += @statement.function_name.bytesize + FIELD_HEADER_SIZE

        function_arg_buffer = ""
        if @statement.function_args && @statement.function_args.length > 0
          function_arg_buffer = Value.of(@statement.function_args).to_bytes
        end
        @data_offset += FIELD_HEADER_SIZE + function_arg_buffer.bytesize
        field_count += 4
      end

      max_records = 0
      parts_full_size = 0
      parts_partial_digest_size = 0
      parts_partial_bval_size = 0

      unless @node_partitions.nil?
        parts_full_size = @node_partitions.parts_full.length * 2
        parts_partial_digest_size = @node_partitions.parts_partial.length * 20

        unless filter.nil?
          parts_partial_bval_size = @node_partitions.parts_partial.length * 8
        end
        max_records = @node_partitions.record_max
      end

      if parts_full_size > 0
        @data_offset += parts_full_size + FIELD_HEADER_SIZE
        field_count += 1
      end

      if parts_partial_digest_size > 0
        @data_offset += parts_partial_digest_size + FIELD_HEADER_SIZE
        field_count += 1
      end

      if parts_partial_bval_size > 0
        @data_offset += parts_partial_bval_size + FIELD_HEADER_SIZE
        field_count += 1
      end

      # Estimate max records field size. This field is used in new servers and not used
      # (but harmless to add) in old servers.
      if max_records > 0
        @data_offset += 8 + FIELD_HEADER_SIZE
        field_count += 1
      end

      operation_count = 0
      unless bin_names.empty?
        # Estimate size for selected bin names (query bin names already handled for old servers).
        bin_names.each do |bin_name|
          estimate_operation_size_for_bin_name(bin_name)
        end
        operation_count = bin_names.length
      end

      projected_offset = @data_offset

      size_buffer

      read_attr = INFO1_READ
      read_attr |= INFO1_NOBINDATA if !@policy.include_bin_data
      read_attr |= INFO1_SHORT_QUERY if @policy.short_query

      infoAttr = INFO3_PARTITION_DONE

      write_header(@policy, read_attr, 0, field_count, operation_count)

      write_field_string(@statement.namespace, FieldType::NAMESPACE) if @statement.namespace
      write_field_string(@statement.set_name, FieldType::TABLE) if @statement.set_name

      # Write records per second.
      write_field_int(@policy.records_per_second, FieldType::RECORDS_PER_SECOND) if @policy.records_per_second > 0

      write_filter_exp(@policy.filter_exp, exp_size)

      # Write socket idle timeout.
      write_field_int(@policy.socket_timeout, FieldType::SOCKET_TIMEOUT)

      # Write task_id field
      write_field_int64(@statement.task_id, FieldType::TRAN_ID)

      unless predexp.nil?
        write_field_header(pred_size, Aerospike::FieldType::PREDEXP)
        @data_offset = Aerospike::PredExp.write(
          predexp, @data_buffer, @data_offset
        )
      end

      if filter
        type = filter.collection_type

        if type > 0
          write_field_header(1, FieldType::INDEX_TYPE)
          @data_offset += @data_buffer.write_byte(type, @data_offset)
        end

        write_field_header(filter_size, FieldType::INDEX_RANGE)
        @data_offset += @data_buffer.write_byte(1, @data_offset)
        @data_offset = filter.write(@data_buffer, @data_offset)

        if packed_ctx
          write_field_header(packed_ctx.length, FieldType::INDEX_CONTEXT)
          @data_offset += @data_buffer.write_binary(packed_ctx, @data_offset)
        end
      end

      if @statement.function_name
        write_field_header(1, FieldType::UDF_OP)
        @data_offset += @data_buffer.write_byte(1, @data_offset)
        write_field_string(@statement.package_name, FieldType::UDF_PACKAGE_NAME)
        write_field_string(@statement.function_name, FieldType::UDF_FUNCTION)
        write_field_string(function_arg_buffer, FieldType::UDF_ARGLIST)
      end

      if parts_full_size > 0
        write_field_header(parts_full_size, FieldType::PID_ARRAY)
        @node_partitions.parts_full.each do |part|
          @data_offset += @data_buffer.write_uint16_little_endian(part.id, @data_offset)
        end
      end

      if parts_partial_digest_size > 0
        write_field_header(parts_partial_digest_size, FieldType::DIGEST_ARRAY)
        @node_partitions.parts_partial.each do |part|
          @data_offset += @data_buffer.write_binary(part.digest, @data_offset)
        end
      end

      if parts_partial_bval_size > 0
        write_field_header(parts_partial_bval_size, FieldType::BVAL_ARRAY)
        @node_partitions.parts_partial.each do |part|
          @data_offset += @data_buffer.write_uint64_little_endian(part.bval, @data_offset)
        end
      end

      if max_records > 0
        write_field(max_records, FieldType::MAX_RECORDS)
      end

      unless bin_names.empty?
        bin_names.each do |bin_name|
          write_operation_for_bin_name(bin_name, Operation::READ)
        end
      end

      end_cmd

      nil
    end

    def should_retry(e)
      # !! converts nil to false
      !!@tracker&.should_retry(@node_partitions, e)
    end
  end # class
end # module
