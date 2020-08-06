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

require 'zlib'

require 'aerospike/record'

require 'aerospike/command/single_command'
require 'aerospike/policy/operate_policy'
require 'aerospike/value/value'

module Aerospike

  private

  class ReadCommand < SingleCommand #:nodoc:
    BIN_NAME_ENCODING = 'utf-8'

    attr_reader :record, :policy

    def initialize(cluster, policy, key, bin_names)
      super(cluster, key)

      @bin_names = bin_names
      @policy = policy

      self
    end

    def get_node
      @cluster.read_node(@partition, @policy.replica, @sequence)
    end

    def write_buffer
      set_read(@policy, @key, @bin_names)
    end

    def parse_result
      # Read header.
      begin
        @conn.read(@data_buffer, 8)
      rescue => e
        Aerospike.logger.error("parse result error: #{e}")
        raise e
      end

      # inflate if compressed
      compressed_sz = compressed_size
      if compressed_sz
        begin
          # waste 8 size bytes
          @conn.read(@data_buffer, 8)

          # read compressed message
          @conn.read(@data_buffer, compressed_sz - 8)

          # inflate the results
          # TODO: reuse the current buffer
          uncompressed = Zlib::inflate(@data_buffer.buf)

          @data_buffer = Buffer.new(-1, uncompressed)
        rescue => e
          Aerospike.logger.error("parse result error: #{e}")
          raise e
        end
      else
        begin
          bytes_read = @conn.read(@data_buffer, MSG_TOTAL_HEADER_SIZE - 8, 8)
        rescue => e
          Aerospike.logger.error("parse result error: #{e}")
          raise e
        end
      end

      # A number of these are commented out because we just don't care enough to read
      # that section of the header. If we do care, uncomment and check!
      sz = @data_buffer.read_int64(0)
      header_length = @data_buffer.read(8).ord
      result_code = @data_buffer.read(13).ord & 0xFF
      generation = @data_buffer.read_int32(14)
      expiration = @data_buffer.read_int32(18)
      field_count = @data_buffer.read_int16(26) # almost certainly 0
      op_count = @data_buffer.read_int16(28)
      receive_size = (sz & 0xFFFFFFFFFFFF) - header_length

      # Read remaining message bytes.
      if compressed_sz 
        @data_buffer.eat!(MSG_TOTAL_HEADER_SIZE)
      elsif receive_size > 0
        size_buffer_sz(receive_size)

        begin
          @conn.read(@data_buffer, receive_size)
        rescue => e
          Aerospike.logger.error("parse result error: #{e}")
          raise e
        end

      end

      if result_code == 0
        if op_count == 0
          @record = Record.new(@node, @key, nil, generation, expiration)
          return
        end
  
        @record = parse_record(op_count, field_count, generation, expiration)
        return
      end

      return nil if result_code == Aerospike::ResultCode::KEY_NOT_FOUND_ERROR

      if result_code == Aerospike::ResultCode::FILTERED_OUT
        if @policy.fail_on_filtered_out
          raise Aerospike::Exceptions::Aerospike.new(result_code)
        end
        return
      end

      if result_code == Aerospike::ResultCode::UDF_BAD_RESPONSE
        begin
          @record = parse_record(op_count, field_count, generation, expiration)
          handle_udf_error(result_code)
        rescue => e
          Aerospike.logger.error("UDF execution error: #{e}")
          raise e
        end
      end

      raise Aerospike::Exceptions::Aerospike.new(result_code)
    end

    def handle_udf_error(result_code)
      ret = @record.bins['FAILURE']
      raise Aerospike::Exceptions::Aerospike.new(result_code, ret) if ret
      raise Aerospike::Exceptions::Aerospike.new(result_code)
    end

    def parse_record(op_count, field_count, generation, expiration)
      bins = op_count > 0 ? {} : nil
      receive_offset = 0
      single_bin_value = (!(OperatePolicy === policy) || policy.record_bin_multiplicity == RecordBinMultiplicity::SINGLE)

      # There can be fields in the response (setname etc).
      # But for now, ignore them. Expose them to the API if needed in the future.
      if field_count > 0
        # Just skip over all the fields
        i = 0
        while i < field_count
          field_size = @data_buffer.read_int32(receive_offset)
          receive_offset += (4 + field_size)
          i = i.succ
        end
      end

      i = 0
      while i < op_count
        op_size = @data_buffer.read_int32(receive_offset)
        particle_type = @data_buffer.read(receive_offset+5).ord
        name_size = @data_buffer.read(receive_offset+7).ord
        name = @data_buffer.read(receive_offset+8, name_size).force_encoding(BIN_NAME_ENCODING)
        receive_offset += 4 + 4 + name_size

        particle_bytes_size = op_size - (4 + name_size)
        value = Aerospike.bytes_to_particle(particle_type, @data_buffer, receive_offset, particle_bytes_size)
        receive_offset += particle_bytes_size

        if single_bin_value || !bins.has_key?(name)
          bins[name] = value
        elsif (prev = bins[name]).kind_of?(OpResults)
          prev << value
        else
          bins[name] = OpResults.new << prev << value
        end

        i = i.succ
      end

      Record.new(@node, @key, bins, generation, expiration)
    end

  end # class

  class OpResults < Array; end

end # module
