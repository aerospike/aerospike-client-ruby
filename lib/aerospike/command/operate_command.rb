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

require "aerospike/command/read_command"

module Aerospike
  private

  class OperateCommand < ReadCommand #:nodoc:
    def initialize(cluster, key, args)
      super(cluster, args.write_policy, key, nil)

      @args = args
    end

    def get_node
      @cluster.master_node(@partition)
    end

    def write_bins
      @operations.select { |op| op.op_type == Aerospike::Operation::WRITE }.map(&:bin).compact
    end

    def write_buffer
      set_operate(@args.write_policy, @key, @args)
    end
  end # class
end # module
