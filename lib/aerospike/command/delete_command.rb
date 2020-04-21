# encoding: utf-8
# Copyright 2014-2017 Aerospike, Inc.
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
require 'aerospike/result_code'

module Aerospike

  private

  class DeleteCommand < SingleCommand #:nodoc:

    attr_reader :existed

    def initialize(cluster, policy, key)
      super(cluster, key)

      @policy = policy

      self
    end

    def write_buffer
      set_delete(@policy, @key)
    end

    def parse_result
      # Read header.
      @conn.read(@data_buffer, MSG_TOTAL_HEADER_SIZE)

      result_code = @data_buffer.read(13).ord & 0xFF

      if result_code == 0
        @existed = true
        return
      end

      if result_code == Aerospike::ResultCode::KEY_NOT_FOUND_ERROR
        @existed = false
        return
      end

      if result_code == Aerospike::ResultCode::FILTERED_OUT
        if @policy.fail_on_filtered_out
          raise Aerospike::Exceptions::Aerospike.new(result_code)
        end
        @existed = true
        return
      end

      raise Aerospike::Exceptions::Aerospike.new(result_code)
    end

  end # class

end # module
