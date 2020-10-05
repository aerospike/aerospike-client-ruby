# encoding: utf-8
# Copyright 2016-2020 Aerospike, Inc.
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

module Aerospike
  module CDT

    # HyperLogLog (HLL) operations.
    # Requires server versions >= 4.9.
    #
    # HyperLogLog operations on HLL items nested in lists/maps are not currently
    # supported by the server.
    class HLLOperation < Operation

      INIT            = 0
      ADD             = 1
      SET_UNION       = 2
      SET_COUNT       = 3
      FOLD            = 4
      COUNT           = 50
      UNION           = 51
      UNION_COUNT     = 52
      INTERSECT_COUNT = 53
      SIMILARITY      = 54
      DESCRIBE        = 55

      attr_reader :hll_op, :values, :return_type, :policy, :index_bit_count, :minhash_bit_count

      def initialize(op_type, hll_op, bin_name, values: nil, index_bit_count: nil, minhash_bit_count: nil, policy: nil)
        @policy = policy
        @op_type = op_type
        @bin_name = bin_name
        @bin_value = nil
        @hll_op = hll_op
        @index_bit_count = index_bit_count
        @minhash_bit_count = minhash_bit_count
        @values = values

        self
      end

      ##
      # Create HLL init operation with minhash bits.
      # Server creates a new HLL or resets an existing HLL.
      # Server does not return a value.
      #
      # policy      write policy, HLLPolicy::DEFAULT is default
      # bin_name     name of bin
      # index_bit_count   number of index bits. Must be between 4 and 16 inclusive.
      # minhash_bit_count   number of min hash bits. Must be between 4 and 58 inclusive.
      def self.init(bin_name, index_bit_count, minhash_bit_count, policy = HLLPolicy::DEFAULT)
        HLLOperation.new(Operation::HLL_MODIFY, INIT, bin_name, index_bit_count: index_bit_count, minhash_bit_count: minhash_bit_count, policy: policy)
      end

      ##
      # Create HLL add operation with minhash bits.
      # Server adds values to HLL set. If HLL bin does not exist, use index_bit_count and minhash_bit_count
      # to create HLL bin. Server returns number of entries that caused HLL to update a register.
      #
      # policy      write policy, HLLPolicy::DEFAULT is default
      # bin_name     name of bin
      # list        list of values to be added
      # index_bit_count   number of index bits. Must be between 4 and 16 inclusive.
      # minhash_bit_count   number of min hash bits. Must be between 4 and 58 inclusive.
      def self.add(bin_name, *values, policy: HLLPolicy::DEFAULT, index_bit_count: -1, minhash_bit_count: -1)
        HLLOperation.new(Operation::HLL_MODIFY, ADD, bin_name, index_bit_count: index_bit_count, minhash_bit_count: minhash_bit_count, values: values, policy: policy)
      end

      ##
      # Create HLL set union operation.
      # Server sets union of specified HLL objects with HLL bin.
      # Server does not return a value.
      #
      # policy      write policy, HLLPolicy::DEFAULT is default
      # bin_name     name of bin
      # list        list of HLL objects
      def self.set_union(bin_name, *values, policy: HLLPolicy::DEFAULT)
        HLLOperation.new(Operation::HLL_MODIFY, SET_UNION, bin_name, values: values, policy: policy)
      end

      ##
      # Create HLL refresh operation.
      # Server updates the cached count (if stale) and returns the count.
      #
      # bin_name     name of bin
      def self.refresh_count(bin_name)
        HLLOperation.new(Operation::HLL_MODIFY, SET_COUNT, bin_name)
      end

      ##
      # Create HLL fold operation.
      # Servers folds index_bit_count to the specified value.
      # This can only be applied when minhash_bit_count on the HLL bin is 0.
      # Server does not return a value.
      #
      # bin_name     name of bin
      # index_bit_count   number of index bits. Must be between 4 and 16 inclusive.
      def self.fold(bin_name, index_bit_count)
        HLLOperation.new(Operation::HLL_MODIFY, FOLD, bin_name, index_bit_count: index_bit_count)
      end

      ##
      # Create HLL getCount operation.
      # Server returns estimated number of elements in the HLL bin.
      #
      # bin_name     name of bin
      def self.get_count(bin_name)
        HLLOperation.new(Operation::HLL_READ, COUNT, bin_name)
      end

      ##
      # Create HLL getUnion operation.
      # Server returns an HLL object that is the union of all specified HLL objects in the list
      # with the HLL bin.
      #
      # bin_name     name of bin
      # list        list of HLL objects
      def self.get_union(bin_name, *values)
        HLLOperation.new(Operation::HLL_READ, UNION, bin_name, values: values)
      end

      ##
      # Create HLL getUnionCount operation.
      # Server returns estimated number of elements that would be contained by the union of these
      # HLL objects.
      # bin_name     name of bin
      # list        list of HLL objects
      def self.get_union_count(bin_name, *values)
        HLLOperation.new(Operation::HLL_READ, UNION_COUNT, bin_name, values: values)
      end

      ##
      # Create HLL getIntersectCount operation.
      # Server returns estimated number of elements that would be contained by the intersection of
      # these HLL objects.
      #
      # bin_name     name of bin
      # list        list of HLL objects
      def self.get_intersect_count(bin_name, *values)
        HLLOperation.new(Operation::HLL_READ, INTERSECT_COUNT, bin_name, values: values)
      end

      ##
      # Create HLL getSimilarity operation.
      # Server returns estimated similarity of these HLL objects. Return type is a double.
      #
      # bin_name     name of bin
      # list        list of HLL objects
      def self.get_similarity(bin_name, *values)
        HLLOperation.new(Operation::HLL_READ, SIMILARITY, bin_name, values: values)
      end

      ##
      # Create HLL describe operation.
      # Server returns index_bit_count and minhash_bit_count used to create HLL bin in a list of longs.
      # The list size is 2.
      #
      # bin_name     name of bin
      def self.describe(bin_name)
        HLLOperation.new(Operation::HLL_READ, DESCRIBE, bin_name)
      end

      def bin_value
        @bin_value ||= pack_bin_value
      end

      private

      def pack_bin_value
        bytes = nil
        Packer.use do |packer|
          args = [hll_op]
          args << values if values
          args << index_bit_count if index_bit_count
          args << minhash_bit_count if minhash_bit_count
          args << policy.flags if policy

          vv = ListValue.new(args)
          vv.pack(packer)
          bytes = packer.bytes
        end
        BytesValue.new(bytes)
      end

    end

  end
end
