# encoding: utf-8
# Copyright 2014-2024 Aerospike, Inc.
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

# Batch key and read only operations with default policy.
# Used in batch read commands where different bins are needed for each key.

module Aerospike

  private

  # Batch key and read/write operations with write policy.
  class BatchWrite < BatchRecord
    # Optional write policy.
    attr_accessor :policy

    # Required operations for this key.
    attr_accessor :ops

    # Initialize batch key and read/write operations.
    #
    # {Operation#get()} is not allowed because it returns a variable number of bins and
    # makes it difficult (sometimes impossible) to lineup operations with results. Instead,
    # use {Operation#get(bin_name)} for each bin name.
    def initialize(key, ops, opt = {})
      super(key, has_write: true)
      @policy = BatchRecord.create_policy(opt, BatchWritePolicy, DEFAULT_BATCH_WRITE_POLICY)
      @ops = ops
    end

    # Optimized reference equality check to determine batch wire protocol repeat flag.
    # For internal use only.
    def ==(other) # :nodoc:
      other && other.instance_of?(self.class) &&
        @ops == other.ops && @policy == other.policy && (@policy.nil? || !@policy.send_key)
    end

    DEFAULT_BATCH_WRITE_POLICY = BatchWritePolicy.new

    # Return wire protocol size. For internal use only.
    def size # :nodoc:
      size = 6 # gen(2) + exp(4) = 6

      size += @policy&.filter_exp&.size if @policy&.filter_exp

      if @policy&.send_key
        size += @key.user_key.estimate_size + Aerospike::FIELD_HEADER_SIZE + 1
      end

      has_write = false
      @ops&.each do |op|
        if op.is_write?
          has_write = true
        end

        size += op.bin_name.bytesize + Aerospike::OPERATION_HEADER_SIZE if op.bin_name
        size += op.bin_value.estimate_size if op.bin_value
      end

      unless has_write
        raise AerospikeException.new(ResultCode::PARAMETER_ERROR, "Batch write operations do not contain a write")
      end

      size
    end
  end
end