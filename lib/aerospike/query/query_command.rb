# encoding: utf-8
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

require 'aerospike/query/stream_command'
require 'aerospike/query/recordset'

module Aerospike

  private

  class QueryCommand < StreamCommand #:nodoc:

    def initialize(node, policy, statement, recordset, partitions)
      super(node)

      @policy = policy
      @statement = statement
      @recordset = recordset
      @partitions = partitions
    end

    def write_buffer
      fieldCount = 0
      filterSize = 0
      binNameSize = 0
      predSize = 0

      begin_cmd

      if @statement.namespace
        @data_offset += @statement.namespace.bytesize + FIELD_HEADER_SIZE
        fieldCount+=1
      end

      if @statement.index_name
        @data_offset += @statement.index_name.bytesize + FIELD_HEADER_SIZE
        fieldCount+=1
      end

      if @statement.set_name
        @data_offset += @statement.set_name.bytesize + FIELD_HEADER_SIZE
        fieldCount+=1
      end

      if !is_scan?
        col_type = @statement.filters[0].collection_type
        if col_type > 0
          @data_offset += FIELD_HEADER_SIZE + 1
          fieldCount += 1
        end

        @data_offset += FIELD_HEADER_SIZE
        filterSize+=1 # num filters

        @statement.filters.each do |filter|
          sz = filter.estimate_size
          filterSize += sz
        end
        @data_offset += filterSize
        fieldCount+=1

        if @statement.bin_names && @statement.bin_names.length > 0
          @data_offset += FIELD_HEADER_SIZE
          binNameSize+=1 # num bin names

          @statement.bin_names.each do |bin_name|
            binNameSize += bin_name.bytesize + 1
          end
          @data_offset += binNameSize
          fieldCount+=1
        end
      else    
        @data_offset += @partitions.length * 2 + FIELD_HEADER_SIZE
        fieldCount += 1

        if @policy.records_per_second > 0
          @data_offset += 4 + FIELD_HEADER_SIZE
          fieldCount += 1
        end

        # Calling query with no filters is more efficiently handled by a primary index scan.
        # Estimate scan options size.
        # @data_offset += (2 + FIELD_HEADER_SIZE)
        # fieldCount+=1
      end

      @statement.set_task_id

      @data_offset += 8 + FIELD_HEADER_SIZE
      fieldCount+=1

      predexp = @policy.predexp || @statement.predexp

      if predexp
        @data_offset += FIELD_HEADER_SIZE
        predSize = Aerospike::PredExp.estimate_size(predexp)
        @data_offset += predSize
        fieldCount += 1
      end

      if @statement.function_name
        @data_offset += FIELD_HEADER_SIZE + 1 # udf type
        @data_offset += @statement.package_name.bytesize + FIELD_HEADER_SIZE
        @data_offset += @statement.function_name.bytesize + FIELD_HEADER_SIZE

        if @statement.function_args && @statement.function_args.length > 0
          functionArgBuffer = Value.of(@statement.function_args).to_bytes
        else
          functionArgBuffer = ''
        end
        @data_offset += FIELD_HEADER_SIZE + functionArgBuffer.bytesize
        fieldCount += 4
      end

      if @statement.filters.nil? || @statement.filters.empty?
        if @statement.bin_names && @statement.bin_names.length > 0
          @statement.bin_names.each do |bin_name|
            estimate_operation_size_for_bin_name(bin_name)
          end
        end
      end

      size_buffer

      readAttr = @policy.include_bin_data ? INFO1_READ : INFO1_READ | INFO1_NOBINDATA
      operation_count = (is_scan? && !@statement.bin_names.nil?) ? @statement.bin_names.length : 0

      write_header(@policy, readAttr, 0, fieldCount, operation_count)

      if @statement.namespace
        write_field_string(@statement.namespace, Aerospike::FieldType::NAMESPACE)
      end

      unless @statement.index_name.nil?
        write_field_string(@statement.index_name, Aerospike::FieldType::INDEX_NAME)
      end

      if @statement.set_name
        write_field_string(@statement.set_name, Aerospike::FieldType::TABLE)
      end

      if !is_scan?
        col_type = @statement.filters[0].collection_type
        if col_type > 0
          write_field_header(1, Aerospike::FieldType::INDEX_TYPE)
          @data_buffer.write_byte(col_type, @data_offset)
          @data_offset+=1
        end

        write_field_header(filterSize, Aerospike::FieldType::INDEX_RANGE)
        @data_buffer.write_byte(@statement.filters.length, @data_offset)
        @data_offset+=1

        @statement.filters.each do |filter|
          @data_offset = filter.write(@data_buffer, @data_offset)
        end

        # Query bin names are specified as a field (Scan bin names are specified later as operations)
        if @statement.bin_names && @statement.bin_names.length > 0
          write_field_header(binNameSize, Aerospike::FieldType::QUERY_BINLIST)
          @data_buffer.write_byte(@statement.bin_names.length, @data_offset)
          @data_offset += 1

          @statement.bin_names.each do |bin_name|
            len = @data_buffer.write_binary(bin_name, @data_offset + 1)
            @data_buffer.write_byte(len, @data_offset)
            @data_offset += len + 1;
          end
        end
      else
        write_field_header(@partitions.length * 2, Aerospike::FieldType::PID_ARRAY)
        for pid in @partitions
          @data_buffer.write_uint16_little_endian(pid, @data_offset)
          @data_offset += 2
        end

        if @policy.records_per_second > 0
          write_field_int(@policy.records_per_second, Aerospike::FieldType::RECORDS_PER_SECOND)
        end

        # Calling query with no filters is more efficiently handled by a primary index scan.
        # write_field_header(2, Aerospike::FieldType::SCAN_OPTIONS)
        # priority = @policy.priority.ord
        # priority = priority << 4
        # @data_buffer.write_byte(priority, @data_offset)
        # @data_offset+=1
        # @data_buffer.write_byte(100.ord, @data_offset)
        # @data_offset+=1
      end

      write_field_header(8, Aerospike::FieldType::TRAN_ID)
      @data_buffer.write_int64(@statement.task_id, @data_offset)
      @data_offset += 8

      if predexp
        write_field_header(predSize, Aerospike::FieldType::PREDEXP)
        @data_offset = Aerospike::PredExp.write(
          predexp, @data_buffer, @data_offset
        )
      end

      if @statement.function_name
        write_field_header(1, Aerospike::FieldType::UDF_OP)
        if @statement.return_data
          @data_buffer.write_byte(1, @data_offset)
          @data_offset+=1
        else
          @data_buffer.write_byte(2, @data_offset)
          @data_offset+=1
        end

        write_field_string(@statement.package_name, Aerospike::FieldType::UDF_PACKAGE_NAME)
        write_field_string(@statement.function_name, Aerospike::FieldType::UDF_FUNCTION)
        write_field_bytes(functionArgBuffer, Aerospike::FieldType::UDF_ARGLIST)
      end

      if is_scan? && !@statement.bin_names.nil?
        @statement.bin_names.each do |bin_name|
          write_operation_for_bin_name(bin_name, Aerospike::Operation::READ)
        end
      end

      end_cmd

      return nil
    end

    def is_scan?
      @statement.filters.nil? || @statement.filters.empty?
    end

  end # class

end # module
