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

require "time"
require "zlib"

require "msgpack"
require "aerospike/result_code"
require "aerospike/command/field_type"

require "aerospike/policy/consistency_level"
require "aerospike/policy/commit_level"

module Aerospike
  private

  # Flags commented out are not supported by cmd client.
  # Contains a read operation.
  INFO1_READ = Integer(1 << 0)
  # Get all bins.
  INFO1_GET_ALL = Integer(1 << 1)
  # Short query
  INFO1_SHORT_QUERY = Integer(1 << 2)

  INFO1_BATCH = Integer(1 << 3)
  # Do not read the bins
  INFO1_NOBINDATA = Integer(1 << 5)

  # Involve all replicas in read operation.
  INFO1_CONSISTENCY_ALL = Integer(1 << 6)

  # Tell server to compress it's response.
  INFO1_COMPRESS_RESPONSE = (1 << 7)

  # Create or update record
  INFO2_WRITE = Integer(1 << 0)
  # Fling a record into the belly of Moloch.
  INFO2_DELETE = Integer(1 << 1)
  # Update if expected generation == old.
  INFO2_GENERATION = Integer(1 << 2)
  # Update if new generation >= old, good for restore.
  INFO2_GENERATION_GT = Integer(1 << 3)
  # Transaction resulting in record deletion leaves tombstone (Enterprise only).
  INFO2_DURABLE_DELETE = Integer(1 << 4)
  # Create only. Fail if record already exists.
  INFO2_CREATE_ONLY = Integer(1 << 5)

  # Return a result for every operation.
  INFO2_RESPOND_ALL_OPS = Integer(1 << 7)

  # This is the last of a multi-part message.
  INFO3_LAST = Integer(1 << 0)
  # Commit to master only before declaring success.
  INFO3_COMMIT_MASTER = Integer(1 << 1)
  # Partition is complete response in scan.
  INFO3_PARTITION_DONE = Integer(1 << 2)
  # Update only. Merge bins.
  INFO3_UPDATE_ONLY = Integer(1 << 3)

  # Create or completely replace record.
  INFO3_CREATE_OR_REPLACE = Integer(1 << 4)
  # Completely replace existing record only.
  INFO3_REPLACE_ONLY = Integer(1 << 5)

  MSG_TOTAL_HEADER_SIZE = 30
  FIELD_HEADER_SIZE = 5
  OPERATION_HEADER_SIZE = 8
  MSG_REMAINING_HEADER_SIZE = 22
  DIGEST_SIZE = 20
  COMPRESS_THRESHOLD = 128
  CL_MSG_VERSION = 2
  AS_MSG_TYPE = 3
  AS_MSG_TYPE_COMPRESSED = 4

  class Command #:nodoc:
    def initialize(node = nil)
      @data_offset = 0
      @data_buffer = nil

      @node = node

      @compress = false

      # will add before use
      @sequence = Atomic.new(-1)

      self
    end

    # List of all bins that this command will write to - sub-classes should
    # override this as appropriate.
    def write_bins
      []
    end

    # Writes the command for write operations
    def set_write(policy, operation, key, bins)
      begin_cmd
      field_count = estimate_key_size(key, policy)

      predexp_size = estimate_predexp(policy.predexp)
      field_count += 1 if predexp_size > 0

      exp_size = estimate_expression_size(@policy.filter_exp)
      field_count += 1 if exp_size > 0

      bins.each do |bin|
        estimate_operation_size_for_bin(bin)
      end

      size_buffer

      write_header_with_policy(policy, 0, INFO2_WRITE, field_count, bins.length)
      write_key(key, policy)
      write_predexp(policy.predexp, predexp_size)
      write_filter_exp(@policy.filter_exp, exp_size)

      bins.each do |bin|
        write_operation_for_bin(bin, operation)
      end

      end_cmd
      mark_compressed(policy)
    end

    # Writes the command for delete operations
    def set_delete(policy, key)
      begin_cmd
      field_count = estimate_key_size(key)

      predexp_size = estimate_predexp(policy.predexp)
      field_count += 1 if predexp_size > 0

      exp_size = estimate_expression_size(@policy.filter_exp)
      field_count += 1 if exp_size > 0

      size_buffer
      write_header_with_policy(policy, 0, INFO2_WRITE | INFO2_DELETE, field_count, 0)
      write_key(key)
      write_predexp(policy.predexp, predexp_size)
      write_filter_exp(@policy.filter_exp, exp_size)
      end_cmd
    end

    # Writes the command for touch operations
    def set_touch(policy, key)
      begin_cmd
      field_count = estimate_key_size(key)

      predexp_size = estimate_predexp(policy.predexp)
      field_count += 1 if predexp_size > 0

      exp_size = estimate_expression_size(@policy.filter_exp)
      field_count += 1 if exp_size > 0

      estimate_operation_size
      size_buffer
      write_header_with_policy(policy, 0, INFO2_WRITE, field_count, 1)
      write_key(key)
      write_predexp(policy.predexp, predexp_size)
      write_filter_exp(@policy.filter_exp, exp_size)
      write_operation_for_operation_type(Aerospike::Operation::TOUCH)
      end_cmd
    end

    # Writes the command for exist operations
    def set_exists(policy, key)
      begin_cmd
      field_count = estimate_key_size(key)

      predexp_size = estimate_predexp(policy.predexp)
      field_count += 1 if predexp_size > 0

      exp_size = estimate_expression_size(@policy.filter_exp)
      field_count += 1 if exp_size > 0

      size_buffer
      write_header(policy, INFO1_READ | INFO1_NOBINDATA, 0, field_count, 0)
      write_key(key)
      write_predexp(policy.predexp, predexp_size)
      write_filter_exp(@policy.filter_exp, exp_size)
      end_cmd
    end

    # Writes the command for get operations (all bins)
    def set_read_for_key_only(policy, key)
      begin_cmd
      field_count = estimate_key_size(key)

      predexp_size = estimate_predexp(policy.predexp)
      field_count += 1 if predexp_size > 0

      exp_size = estimate_expression_size(@policy.filter_exp)
      field_count += 1 if exp_size > 0

      size_buffer
      write_header(policy, INFO1_READ | INFO1_GET_ALL, 0, field_count, 0)
      write_key(key)
      write_predexp(policy.predexp, predexp_size)
      write_filter_exp(@policy.filter_exp, exp_size)
      end_cmd
    end

    # Writes the command for get operations (specified bins)
    def set_read(policy, key, bin_names)
      if bin_names && bin_names.length > 0
        begin_cmd
        field_count = estimate_key_size(key)

        predexp_size = estimate_predexp(policy.predexp)
        field_count += 1 if predexp_size > 0

        exp_size = estimate_expression_size(@policy.filter_exp)
        field_count += 1 if exp_size > 0

        bin_names.each do |bin_name|
          estimate_operation_size_for_bin_name(bin_name)
        end

        size_buffer
        write_header(policy, INFO1_READ, 0, field_count, bin_names.length)
        write_key(key)
        write_predexp(policy.predexp, predexp_size)
        write_filter_exp(@policy.filter_exp, exp_size)

        bin_names.each do |bin_name|
          write_operation_for_bin_name(bin_name, Aerospike::Operation::READ)
        end

        end_cmd
      else
        set_read_for_key_only(policy, key)
      end
    end

    # Writes the command for getting metadata operations
    def set_read_header(policy, key)
      begin_cmd
      field_count = estimate_key_size(key)

      predexp_size = estimate_predexp(policy.predexp)
      field_count += 1 if predexp_size > 0

      exp_size = estimate_expression_size(@policy.filter_exp)
      field_count += 1 if exp_size > 0

      estimate_operation_size_for_bin_name("")
      size_buffer

      # The server does not currently return record header data with _INFO1_NOBINDATA attribute set.
      # The workaround is to request a non-existent bin.
      # TODO: Fix this on server.
      #command.set_read(INFO1_READ | _INFO1_NOBINDATA);
      write_header(policy, INFO1_READ, 0, field_count, 1)

      write_key(key)
      write_predexp(policy.predexp, predexp_size)
      write_filter_exp(@policy.filter_exp, exp_size)
      write_operation_for_bin_name("", Aerospike::Operation::READ)
      end_cmd
    end

    # Implements different command operations
    def set_operate(policy, key, args)
      begin_cmd
      field_count = estimate_key_size(key, policy)

      predexp_size = estimate_predexp(policy.predexp)
      field_count += 1 if predexp_size > 0

      exp_size = estimate_expression_size(policy.filter_exp)
      field_count += 1 if exp_size > 0

      @data_offset += args.size

      size_buffer

      write_header_with_policy(policy, args.read_attr, args.write_attr, field_count, args.operations.length)
      write_key(key, policy)
      write_predexp(policy.predexp, predexp_size)
      write_filter_exp(policy.filter_exp, exp_size)

      args.operations.each do |operation|
        write_operation_for_operation(operation)
      end

      end_cmd
      mark_compressed(policy)
    end

    def set_udf(policy, key, package_name, function_name, args)
      begin_cmd
      field_count = estimate_key_size(key, policy)

      predexp_size = estimate_predexp(policy.predexp)
      field_count += 1 if predexp_size > 0

      exp_size = estimate_expression_size(@policy.filter_exp)
      field_count += 1 if exp_size > 0

      arg_bytes = args.to_bytes

      field_count += estimate_udf_size(package_name, function_name, arg_bytes)
      size_buffer

      write_header(policy, 0, INFO2_WRITE, field_count, 0)
      write_key(key, policy)
      write_predexp(policy.predexp, predexp_size)
      write_filter_exp(@policy.filter_exp, exp_size)
      write_field_string(package_name, Aerospike::FieldType::UDF_PACKAGE_NAME)
      write_field_string(function_name, Aerospike::FieldType::UDF_FUNCTION)
      write_field_bytes(arg_bytes, Aerospike::FieldType::UDF_ARGLIST)

      end_cmd
      mark_compressed(policy)
    end

    def set_scan(policy, namespace, set_name, bin_names, node_partitions)
      # Estimate buffer size
      begin_cmd
      field_count = 0

      parts_full_size = node_partitions.parts_full.length * 2
      parts_partial_size = node_partitions.parts_partial.length * 20
      max_records = node_partitions.record_max

      if namespace
        @data_offset += namespace.bytesize + FIELD_HEADER_SIZE
        field_count += 1
      end

      if set_name
        @data_offset += set_name.bytesize + FIELD_HEADER_SIZE
        field_count += 1
      end

      if parts_full_size > 0
        @data_offset += parts_full_size + FIELD_HEADER_SIZE
        field_count += 1
      end

      if parts_partial_size > 0
        @data_offset += parts_partial_size + FIELD_HEADER_SIZE
        field_count += 1
      end

      if max_records > 0
        @data_offset += 8 + FIELD_HEADER_SIZE
        field_count += 1
      end

      if policy.records_per_second > 0
        @data_offset += 4 + FIELD_HEADER_SIZE
        field_count += 1
      end

      predexp_size = estimate_predexp(policy.predexp)
      field_count += 1 if predexp_size > 0

      exp_size = estimate_expression_size(@policy.filter_exp)
      field_count += 1 if exp_size > 0

      # Estimate scan options size.
      # @data_offset += 2 + FIELD_HEADER_SIZE
      # field_count += 1

      # Estimate scan timeout size.
      @data_offset += 4 + FIELD_HEADER_SIZE
      field_count += 1

      if bin_names
        bin_names.each do |bin_name|
          estimate_operation_size_for_bin_name(bin_name)
        end
      end

      size_buffer
      read_attr = INFO1_READ

      unless policy.include_bin_data
        read_attr |= INFO1_NOBINDATA
      end

      operation_count = 0
      if bin_names
        operation_count = bin_names.length
      end

      write_header(policy, read_attr, 0, field_count, operation_count)

      if namespace
        write_field_string(namespace, Aerospike::FieldType::NAMESPACE)
      end

      if set_name
        write_field_string(set_name, Aerospike::FieldType::TABLE)
      end

      if parts_full_size > 0
        write_field_header(parts_full_size, Aerospike::FieldType::PID_ARRAY)

        node_partitions.parts_full.each do |part|
          @data_buffer.write_uint16_little_endian(part.id, @data_offset)
          @data_offset += 2
        end
      end

      if parts_partial_size > 0
        write_field_header(parts_partial_size, Aerospike::FieldType::DIGEST_ARRAY)

        node_partitions.parts_partial.each do |part|
          @data_buffer.write_binary(part.digest, @data_offset)
          @data_offset += part.digest.length
        end
      end

      if max_records > 0
        write_field_int64(max_records, Aerospike::FieldType::MAX_RECORDS)
      end

      if policy.records_per_second > 0
        write_field_int(policy.records_per_second, Aerospike::FieldType::RECORDS_PER_SECOND)
      end

      write_predexp(policy.predexp, predexp_size)
      write_filter_exp(@policy.filter_exp, exp_size)

      # write_field_header(2, Aerospike::FieldType::SCAN_OPTIONS)

      # priority = policy.priority & 0xFF
      # priority <<= 4
      # if policy.fail_on_cluster_change
      #   priority |= 0x08
      # end

      # @data_buffer.write_byte(priority, @data_offset)
      # @data_offset += 1
      # @data_buffer.write_byte(policy.scan_percent.to_i.ord, @data_offset)
      # @data_offset += 1

      write_field_header(4, Aerospike::FieldType::SOCKET_TIMEOUT)
      @data_buffer.write_uint32(policy.socket_timeout.to_i, @data_offset)
      @data_offset += 4

      if bin_names
        bin_names.each do |bin_name|
          write_operation_for_bin_name(bin_name, Aerospike::Operation::READ)
        end
      end

      end_cmd
    end

    def execute
      iterations = 0

      # set timeout outside the loop
      limit = Time.now + @policy.timeout

      # Execute command until successful, timed out or maximum iterations have been reached.
      while true
        # too many retries
        iterations += 1
        break if (@policy.max_retries > 0) && (iterations > @policy.max_retries + 1)

        # Sleep before trying again, after the first iteration
        sleep(@policy.sleep_between_retries) if iterations > 1 && @policy.sleep_between_retries > 0

        # check for command timeout
        break if @policy.timeout > 0 && Time.now > limit

        begin
          @node = get_node
          @conn = @node.get_connection(@policy.timeout)
        rescue => e
          if e.is_a?(Aerospike::Exceptions::MaxConnectionsExceeded)
            Aerospike.logger.error("Maximum connections established. No new connection can be created. #{e}")
          end
          if @node
            # Socket connection error has occurred. Decrease health and retry.
            @node.decrease_health

            Aerospike.logger.error("Node #{@node.to_s}: #{e}")
          else
            Aerospike.logger.error("No node available for transaction: #{e}")
          end
          next
        end

        # Draw a buffer from buffer pool, and make sure it will be put back
        begin
          @data_buffer = Buffer.get

          # Set command buffer.
          begin
            write_buffer
          rescue => e
            Aerospike.logger.error(e)

            # All runtime exceptions are considered fatal. Do not retry.
            # Close socket to flush out possible garbage. Do not put back in pool.
            @node.close_connection(@conn) if @conn
            raise e
          end

          # Reset timeout in send buffer (destined for server) and socket.
          @data_buffer.write_int32((@policy.timeout * 1000).to_i, 22)

          # Send command.
          begin
            @conn.write(@data_buffer, @data_offset)
          rescue => e
            # IO errors are considered temporary anomalies. Retry.
            # Close socket to flush out possible garbage. Do not put back in pool.
            @node.close_connection(@conn) if @conn

            Aerospike.logger.error("Node #{@node.to_s}: #{e}")
            # IO error means connection to server @node is unhealthy.
            # Reflect cmd status.
            @node.decrease_health
            next
          end

          # Parse results.
          begin
            parse_result
          rescue => e
            case e
            # do not log the following exceptions
            when Aerospike::Exceptions::ScanTerminated
            when Aerospike::Exceptions::QueryTerminated
            else
              Aerospike.logger.error(e)
            end

            # close the connection
            # cancelling/closing the batch/multi commands will return an error, which will
            # close the connection to throw away its data and signal the server about the
            # situation. We will not put back the connection in the buffer.
            @node.close_connection(@conn) if @conn
            raise e
          end

          # Reflect healthy status.
          @node.restore_health

          # Put connection back in pool.
          @node.put_connection(@conn)

          # command has completed successfully.  Exit method.
          return
        ensure
          Buffer.put(@data_buffer)
        end
      end # while

      # execution timeout
      raise Aerospike::Exceptions::Timeout.new(limit, iterations)
    end

    protected

    def estimate_key_size(key, policy = nil)
      field_count = 0

      if key.namespace
        @data_offset += key.namespace.bytesize + FIELD_HEADER_SIZE
        field_count += 1
      end

      if key.set_name
        @data_offset += key.set_name.bytesize + FIELD_HEADER_SIZE
        field_count += 1
      end

      @data_offset += key.digest.bytesize + FIELD_HEADER_SIZE
      field_count += 1

      if policy && policy.respond_to?(:send_key) && policy.send_key == true
        # field header size + key size
        @data_offset += key.user_key_as_value.estimate_size + FIELD_HEADER_SIZE
        field_count += 1
      end

      return field_count
    end

    def estimate_udf_size(package_name, function_name, bytes)
      @data_offset += package_name.bytesize + FIELD_HEADER_SIZE
      @data_offset += function_name.bytesize + FIELD_HEADER_SIZE
      @data_offset += bytes.bytesize + FIELD_HEADER_SIZE
      return 3
    end

    def estimate_operation_size_for_bin(bin)
      @data_offset += bin.name.bytesize + OPERATION_HEADER_SIZE
      @data_offset += bin.value_object.estimate_size
    end

    def estimate_operation_size_for_operation(operation)
      bin_len = 0

      if operation.bin_name
        bin_len = operation.bin_name.bytesize
      end

      @data_offset += bin_len + OPERATION_HEADER_SIZE

      if operation.bin_value
        @data_offset += operation.bin_value.estimate_size
      end
    end

    def estimate_operation_size_for_bin_name(bin_name)
      @data_offset += bin_name.bytesize + OPERATION_HEADER_SIZE
    end

    def estimate_operation_size
      @data_offset += OPERATION_HEADER_SIZE
    end

    def estimate_predexp(predexp)
      if predexp && predexp.size > 0
        @data_offset += FIELD_HEADER_SIZE
        sz = Aerospike::PredExp.estimate_size(predexp)
        @data_offset += sz
        return sz
      end
      return 0
    end

    def estimate_expression_size(exp)
      unless exp.nil?
        @data_offset += FIELD_HEADER_SIZE
        @data_offset += exp.size
        return exp.size
      end
      0
    end

    # Generic header write.
    def write_header(policy, read_attr, write_attr, field_count, operation_count)
      read_attr |= INFO1_CONSISTENCY_ALL if policy.consistency_level == Aerospike::ConsistencyLevel::CONSISTENCY_ALL
      read_attr |= INFO1_COMPRESS_RESPONSE if policy.use_compression

      # Write all header data except total size which must be written last.
      @data_buffer.write_byte(MSG_REMAINING_HEADER_SIZE, 8) # Message heade.length.
      @data_buffer.write_byte(read_attr, 9)
      @data_buffer.write_byte(write_attr, 10)

      i = 11
      while i <= 25
        @data_buffer.write_byte(0, i)
        i = i.succ
      end

      @data_buffer.write_int16(field_count, 26)
      @data_buffer.write_int16(operation_count, 28)

      @data_offset = MSG_TOTAL_HEADER_SIZE
    end

    # Header write for write operations.
    def write_header_with_policy(policy, read_attr, write_attr, field_count, operation_count)
      # Set flags.
      generation = Integer(0)
      info_attr = Integer(0)

      case policy.record_exists_action
      when Aerospike::RecordExistsAction::UPDATE
      when Aerospike::RecordExistsAction::UPDATE_ONLY
        info_attr |= INFO3_UPDATE_ONLY
      when Aerospike::RecordExistsAction::REPLACE
        info_attr |= INFO3_CREATE_OR_REPLACE
      when Aerospike::RecordExistsAction::REPLACE_ONLY
        info_attr |= INFO3_REPLACE_ONLY
      when Aerospike::RecordExistsAction::CREATE_ONLY
        write_attr |= INFO2_CREATE_ONLY
      end

      case policy.generation_policy
      when Aerospike::GenerationPolicy::NONE
      when Aerospike::GenerationPolicy::EXPECT_GEN_EQUAL
        generation = policy.generation
        write_attr |= INFO2_GENERATION
      when Aerospike::GenerationPolicy::EXPECT_GEN_GT
        generation = policy.generation
        write_attr |= INFO2_GENERATION_GT
      end

      info_attr |= INFO3_COMMIT_MASTER if policy.commit_level == Aerospike::CommitLevel::COMMIT_MASTER
      read_attr |= INFO1_CONSISTENCY_ALL if policy.consistency_level == Aerospike::ConsistencyLevel::CONSISTENCY_ALL
      write_attr |= INFO2_DURABLE_DELETE if policy.durable_delete
      read_attr |= INFO1_COMPRESS_RESPONSE if policy.use_compression

      # Write all header data except total size which must be written last.
      @data_buffer.write_byte(MSG_REMAINING_HEADER_SIZE, 8) # Message heade.length.
      @data_buffer.write_byte(read_attr, 9)
      @data_buffer.write_byte(write_attr, 10)
      @data_buffer.write_byte(info_attr, 11)
      @data_buffer.write_byte(0, 12) # unused
      @data_buffer.write_byte(0, 13) # clear the result code
      @data_buffer.write_uint32(generation, 14)
      @data_buffer.write_uint32(policy.ttl, 18)

      # Initialize timeout. It will be written later.
      @data_buffer.write_byte(0, 22)
      @data_buffer.write_byte(0, 23)
      @data_buffer.write_byte(0, 24)
      @data_buffer.write_byte(0, 25)

      @data_buffer.write_int16(field_count, 26)
      @data_buffer.write_int16(operation_count, 28)

      @data_offset = MSG_TOTAL_HEADER_SIZE
    end

    def write_key(key, policy = nil)
      # Write key into buffer.
      if key.namespace
        write_field_string(key.namespace, Aerospike::FieldType::NAMESPACE)
      end

      if key.set_name
        write_field_string(key.set_name, Aerospike::FieldType::TABLE)
      end

      write_field_bytes(key.digest, Aerospike::FieldType::DIGEST_RIPE)

      if policy && policy.respond_to?(:send_key) && policy.send_key == true
        write_field_value(key.user_key_as_value, Aerospike::FieldType::KEY)
      end
    end

    def write_operation_for_bin(bin, operation)
      name_length = @data_buffer.write_binary(bin.name, @data_offset + OPERATION_HEADER_SIZE)
      value_length = bin.value_object.write(@data_buffer, @data_offset + OPERATION_HEADER_SIZE + name_length)

      # Buffer.Int32ToBytes(name_length+value_length+4, @data_buffer, @data_offset)
      @data_buffer.write_int32(name_length + value_length + 4, @data_offset)

      @data_offset += 4
      @data_buffer.write_byte(operation, @data_offset)
      @data_offset += 1
      @data_buffer.write_byte(bin.value_object.type, @data_offset)
      @data_offset += 1
      @data_buffer.write_byte(0, @data_offset)
      @data_offset += 1
      @data_buffer.write_byte(name_length, @data_offset)
      @data_offset += 1
      @data_offset += name_length + value_length
    end

    def write_operation_for_operation(operation)
      name_length = 0
      if operation.bin_name
        name_length = @data_buffer.write_binary(operation.bin_name, @data_offset + OPERATION_HEADER_SIZE)
      end

      value_length = operation.bin_value.write(@data_buffer, @data_offset + OPERATION_HEADER_SIZE + name_length)

      # Buffer.Int32ToBytes(name_length+value_length+4, @data_buffer, @data_offset)
      @data_buffer.write_int32(name_length + value_length + 4, @data_offset)

      @data_offset += 4
      @data_buffer.write_byte(operation.op_type, @data_offset)
      @data_offset += 1
      @data_buffer.write_byte(operation.bin_value.type, @data_offset)
      @data_offset += 1
      @data_buffer.write_byte(0, @data_offset)
      @data_offset += 1
      @data_buffer.write_byte(name_length, @data_offset)
      @data_offset += 1
      @data_offset += name_length + value_length
    end

    def write_operation_for_bin_name(name, operation)
      name_length = @data_buffer.write_binary(name, @data_offset + OPERATION_HEADER_SIZE)
      # Buffer.Int32ToBytes(name_length+4, @data_buffer, @data_offset)
      @data_buffer.write_int32(name_length + 4, @data_offset)

      @data_offset += 4
      @data_buffer.write_byte(operation, @data_offset)
      @data_offset += 1
      @data_buffer.write_byte(0, @data_offset)
      @data_offset += 1
      @data_buffer.write_byte(0, @data_offset)
      @data_offset += 1
      @data_buffer.write_byte(name_length, @data_offset)
      @data_offset += 1
      @data_offset += name_length
    end

    def write_operation_for_operation_type(operation)
      # Buffer.Int32ToBytes(4), @data_buffer, @data_offset
      @data_buffer.write_int32(4, @data_offset)
      @data_offset += 4
      @data_buffer.write_byte(operation, @data_offset)
      @data_offset += 1
      @data_buffer.write_byte(0, @data_offset)
      @data_offset += 1
      @data_buffer.write_byte(0, @data_offset)
      @data_offset += 1
      @data_buffer.write_byte(0, @data_offset)
      @data_offset += 1
    end

    def write_field_value(value, ftype)
      offset = @data_offset + FIELD_HEADER_SIZE
      @data_buffer.write_byte(value.type, offset)
      offset += 1
      len = value.write(@data_buffer, offset)
      len += 1
      write_field_header(len, ftype)
      @data_offset += len
    end

    def write_field_string(str, ftype)
      len = @data_buffer.write_binary(str, @data_offset + FIELD_HEADER_SIZE)
      write_field_header(len, ftype)
      @data_offset += len
    end

    def write_u16_little_endian(i, ftype)
      @data_buffer.write_uint16_little_endian(i, @data_offset + FIELD_HEADER_SIZE)
      write_field_header(2, ftype)
      @data_offset += 2
    end

    def write_field_int(i, ftype)
      @data_buffer.write_int32(i, @data_offset + FIELD_HEADER_SIZE)
      write_field_header(4, ftype)
      @data_offset += 4
    end

    def write_field_int64(i, ftype)
      @data_buffer.write_int64(i, @data_offset + FIELD_HEADER_SIZE)
      write_field_header(8, ftype)
      @data_offset += 8
    end

    def write_field_bytes(bytes, ftype)
      @data_buffer.write_binary(bytes, @data_offset + FIELD_HEADER_SIZE)
      write_field_header(bytes.bytesize, ftype)
      @data_offset += bytes.bytesize
    end

    def write_field_header(size, ftype)
      @data_buffer.write_int32(size + 1, @data_offset)
      @data_offset += 4
      @data_buffer.write_byte(ftype, @data_offset)
      @data_offset += 1
    end

    def write_predexp(predexp, predexp_size)
      if predexp && predexp.size > 0
        write_field_header(predexp_size, Aerospike::FieldType::FILTER_EXP)
        @data_offset = Aerospike::PredExp.write(
          predexp, @data_buffer, @data_offset
        )
      end
    end

    def write_filter_exp(exp, exp_size)
      unless exp.nil?
        write_field_header(exp_size, Aerospike::FieldType::FILTER_EXP)
        @data_offset += exp.write(@data_buffer, @data_offset)
      end
    end

    def begin_cmd
      @data_offset = MSG_TOTAL_HEADER_SIZE
    end

    def size_buffer
      size_buffer_sz(@data_offset)
    end

    def size_buffer_sz(size)
      @data_buffer.resize(size)
    end

    def end_cmd
      size = (@data_offset - 8) | Integer(CL_MSG_VERSION << 56) | Integer(AS_MSG_TYPE << 48)
      @data_buffer.write_int64(size, 0)
    end

    def use_compression?
      @compress
    end

    def compress_buffer
      if @data_offset > COMPRESS_THRESHOLD
        compressed = Zlib::deflate(@data_buffer.buf, Zlib::DEFAULT_COMPRESSION)

        # write original size as header
        proto_s = "%08d" % 0
        proto_s[0, 8] = [@data_offset].pack("q>")
        compressed.prepend(proto_s)

        # write proto
        proto = (compressed.size + 8) | Integer(CL_MSG_VERSION << 56) | Integer(AS_MSG_TYPE << 48)
        proto_s = "%08d" % 0
        proto_s[0, 8] = [proto].pack("q>")
        compressed.prepend(proto_s)

        @data_buffer = Buffer.new(-1, compressed)
        @data_offset = @data_buffer.size
      end
    end

    # isCompressed returns the length of the compressed buffer.
    # If the buffer is not compressed, the result will be -1
    def compressed_size
      # A number of these are commented out because we just don't care enough to read
      # that section of the header. If we do care, uncomment and check!
      proto = @data_buffer.read_int64(0)
      size = proto & 0xFFFFFFFFFFFF
      msg_type = (proto >> 48) & 0xFF

      return nil if msg_type != AS_MSG_TYPE_COMPRESSED

      size
    end

    def mark_compressed(policy)
      @compress = policy.use_compression
    end
  end # class
end # module
