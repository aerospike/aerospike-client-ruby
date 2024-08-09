# encoding: utf-8
# Copyright 2014-2024 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License") you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

# Batch key and read only operations with default policy.
# Used in batch read commands where different bins are needed for each key.

module Aerospike

  class BatchRead < BatchRecord

    # Optional read policy.
    attr_accessor :policy

    # Bins to retrieve for this key. bin_names are mutually exclusive with
    # {BatchRead#ops}.
    attr_accessor :bin_names

    # Optional operations for this key. ops are mutually exclusive with
    # {BatchRead#bin_names}. A bin_name can be emulated with
    # {Operation#get(bin_name)}
    attr_accessor :ops

    # If true, ignore bin_names and read all bins.
    # If false and bin_names are set, read specified bin_names.
    # If false and bin_names are not set, read record header (generation, expiration) only.
    attr_accessor :read_all_bins

    # Initialize batch key and bins to retrieve.
    def self.read_bins(key, bin_names, opt = {})
      br = BatchRead.new(key)
      br.policy = BatchRecord.create_policy(opt, BatchReadPolicy, DEFAULT_BATCH_READ_POLICY)
      br.bin_names = bin_names
      br.read_all_bins = false
      br
    end

    # Initialize batch key and read_all_bins indicator.
    def self.read_all_bins(key, opt = {})
      br = BatchRead.new(key)
      br.policy = create_policy(opt, BatchReadPolicy, DEFAULT_BATCH_READ_POLICY)
      br.read_all_bins = true
      br
    end

    # Initialize batch key and read operations.
    def self.ops(key, ops, opt = {})
      br = BatchRead.new(key)
      br.policy = create_policy(opt, BatchReadPolicy, DEFAULT_BATCH_READ_POLICY)
      br.ops = ops
      br.read_all_bins = false
      br
    end

    # Optimized reference equality check to determine batch wire protocol repeat flag.
    # For internal use only.
    def ==(other) # :nodoc:
      other && other.instance_of?(self.class) &&
        @bin_names.sort == other.bin_names.sort && @ops.sort == other.ops.sort &&
        @policy == other.policy && @read_all_bins == other.read_all_bins
    end

    DEFAULT_BATCH_READ_POLICY = BatchReadPolicy.new

    # Return wire protocol size. For internal use only.
    def size # :nodoc:
      size = 0
      size += @policy&.filter_exp&.size if @policy&.filter_exp

      @bin_names&.each do |bin_name|
        size += bin_name.bytesize + Aerospike::OPERATION_HEADER_SIZE
      end

      @ops&.each do |op|
        if op.is_write?
          raise AerospikeException.new(ResultCode::PARAMETER_ERROR, "Write operations not allowed in batch read")
        end
        size += op.bin_name.bytesize + Aerospike::OPERATION_HEADER_SIZE
        size += op.value.estimate_size
      end

      size
    end
  end
end
