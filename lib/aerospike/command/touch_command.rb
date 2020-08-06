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

require 'aerospike/command/single_command'

module Aerospike

  private

  class TouchCommand < SingleCommand #:nodoc:

    def initialize(cluster, policy, key)
      super(cluster, key)

      @policy = policy

      self
    end

    def get_node
      @cluster.master_node(@partition)
    end

    def write_buffer
      set_touch(@policy, @key)
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
          #waste 8 size bytes
          @conn.read(@data_buffer, 8)

          # read compressed message
          @conn.read(@data_buffer, sz - 8)

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

      result_code = @data_buffer.read(13).ord & 0xFF

      return if result_code == 0

      if result_code == Aerospike::ResultCode::FILTERED_OUT
        if @policy.fail_on_filtered_out
          raise Aerospike::Exceptions::Aerospike.new(result_code)
        end
        return
      end

      raise Aerospike::Exceptions::Aerospike.new(result_code)
    end

  end # class

end # module
