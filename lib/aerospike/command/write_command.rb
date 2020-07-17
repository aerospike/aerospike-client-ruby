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

  class WriteCommand < SingleCommand #:nodoc:

    def initialize(cluster, policy, key, bins, operation)

      super(cluster, key)

      @bins =          bins
      @operation =     operation
      @policy = policy

      self
    end

    def get_node
      @cluster.master_node(@partition)
    end

    def write_bins
      @bins
    end

    def write_buffer
      set_write(@policy, @operation, @key, @bins)
    end

    def parse_result
      # Read header.
      begin
        @conn.read(@data_buffer, MSG_TOTAL_HEADER_SIZE)
      rescue => e
        Aerospike.logger.error(e)
        raise e
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
