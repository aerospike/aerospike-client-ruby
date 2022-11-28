# encoding: utf-8
# Copyright 2016-2020 Aerospike, Inc.
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

require "aerospike/operation"

module Aerospike
  private

  class OperateArgs
    attr_reader :write_policy, :operations, :partition
    attr_reader :size, :read_attr, :write_attr, :has_write

    RESPOND_ALL_OPS_READ_CMDS = [Operation::BIT_READ, Operation::EXP_READ, Operation::HLL_READ, Operation::CDT_READ]
    READ_CMDS = [Operation::BIT_READ, Operation::EXP_READ, Operation::HLL_READ, Operation::CDT_READ, Operation::CDT_READ, Operation::READ]
    MODIFY_CMDS = [Operation::BIT_MODIFY, Operation::EXP_MODIFY, Operation::HLL_MODIFY, Operation::CDT_MODIFY]

    def initialize(cluster, policy, write_default, read_default, key, operations)
      @operations = operations

      data_offset = 0
      rattr = 0
      wattr = 0
      write = false
      read_bin = false
      read_header = false
      respond_all_ops = false

      @operations.each do |operation|
        if READ_CMDS.include?(operation.op_type)
          if RESPOND_ALL_OPS_READ_CMDS.include?(operation.op_type)
            # Map @operations require respond_all_ops to be true.
            respond_all_ops = true
          end

          rattr |= Aerospike::INFO1_READ

          # Read all bins if no bin is specified.
          rattr |= Aerospike::INFO1_GET_ALL if operation.bin_name.nil?
          read_bin = true
        elsif operation.op_type == Operation::READ_HEADER
          rattr |= Aerospike::INFO1_READ
          read_header = true
        elsif MODIFY_CMDS.include?(operation.op_type)
          # Map @operations require respond_all_ops to be true.
          respond_all_ops = true

          wattr = Aerospike::INFO2_WRITE
          write = true
        else
          wattr = Aerospike::INFO2_WRITE
          write = true
        end
        data_offset += operation.bin_name.bytesize + Aerospike::OPERATION_HEADER_SIZE unless operation.bin_name.nil?
        data_offset += operation.bin_value.estimate_size
      end

      @size = data_offset
      @has_write = write

      if read_header && !read_bin
        rattr |= Aerospike::INFO1_NOBINDATA
      end
      @read_attr = rattr

      if policy.nil?
        @write_policy = write ? write_default : read_default
      else
        @write_policy = policy
      end

      # When GET_ALL is specified, RESPOND_ALL_OPS must be disabled.
      if (respond_all_ops && policy.record_bin_multiplicity) && (rattr & Aerospike::INFO1_GET_ALL) == 0
        wattr |= Aerospike::INFO2_RESPOND_ALL_OPS
      end
      @write_attr = wattr

      if write
        # @partition = Partition.write(cluster, @write_policy, key)
        @partition = Partition.new_by_key(key)
      else
        # @partition = Partition.read(cluster, @write_policy, key)
        @partition = Partition.new_by_key(key)
      end
    end
  end
end
