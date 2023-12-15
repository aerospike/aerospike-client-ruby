# encoding: utf-8
# Copyright 2014-2023 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.


module Aerospike
  class ServerCommand < MultiCommand
    attr_accessor :statement, :task_id, :cluster, :write_policy, :background

    def initialize(cluster, node, policy, statement, background, task_id)
      super(node)
      @statement = statement
      @task_id = task_id
      @cluster = cluster
      @policy = policy
      @background = background
    end

    def write?
      true
    end

    def write_buffer
      set_query(@cluster, @policy, @statement, true, nil)
    end

    def parse_row(result_code)
      field_count = @data_buffer.read_int16(18)
      result_code = @data_buffer.read(5).ord & 0xFF
      skip_key(field_count)

      if result_code != 0
        if result_code == Aerospike::ResultCode::KEY_NOT_FOUND_ERROR
          return false
        end
        raise Aerospike::Exceptions::Aerospike.new(result_code)
      end
      op_count = @data_buffer.read_int16(20)
      if op_count <= 0
        return Record.new(@node, key, bins, generation, expiration)
      end

      unless valid?
        raise Aerospike::Exceptions::QueryTerminated
      end
    end
  end
end