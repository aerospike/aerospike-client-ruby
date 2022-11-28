# encoding: utf-8
# Copyright 2014-2022 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License") you may no
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

module Aerospike
  # HyperLogLog (HLL) expression generator. See {@link Exp}.
  #
  # The bin expression argument in these methods can be a reference to a bin or the
  # result of another expression. Expressions that modify bin values are only used
  # for temporary expression evaluation and are not permanently applied to the bin.
  # HLL modify expressions return the HLL bin's value.
  class Exp::HLL

    # Create expression that creates a new HLL or resets an existing HLL with minhash bits.
    #
    # @param policy			write policy, use {@link HLLPolicy#Default} for default
    # @param index_bit_count		number of index bits. Must be between 4 and 16 inclusive.
    # @param min_hash_bit_count	number of min hash bits. Must be between 4 and 51 inclusive.
    # 							Also, index_bit_count + min_hash_bit_count must be <= 64. Optional.
    # @param bin				HLL bin or value expression
    def self.init(index_bit_count, bin, min_hash_bit_count: Exp.int_val(-1), policy: CDT::HLLPolicy::DEFAULT)
      bytes = Exp.pack(nil, INIT, index_bit_count, min_hash_bit_count, policy.flags)
      self.add_write(bin, bytes)
    end

    # Create expression that adds values to a HLL set and returns HLL set. If HLL bin does not
    # exist, use index_bit_count and min_hash_bit_count to create HLL set.
    #
    # ==== Examples
    # # Add values to HLL bin "a" and check count > 7
    # Exp.gt(
    #   HLLExp.getCount(
    #     HLLExp.add(HLLPolicy.Default, Exp.val(list), Exp.val(10), Exp.val(20), Exp.hllBin("a"))),
    #   Exp.val(7))
    #
    # @param policy			write policy, use {@link HLLPolicy#Default} for default
    # @param list				list bin or value expression of values to be added
    # @param index_bit_count		number of index bits expression. Must be between 4 and 16 inclusive.
    # @param min_hash_bit_count   number of min hash bits expression. Must be between 4 and 51 inclusive.
    # 							Also, index_bit_count + min_hash_bit_count must be <= 64.
    # @param bin				HLL bin or value expression
    def self.add(list, bin, policy: CDT::HLLPolicy::DEFAULT, index_bit_count: Exp.val(-1), min_hash_bit_count: Exp.val(-1))
      bytes = Exp.pack(nil, ADD, list, index_bit_count, min_hash_bit_count, policy.flags)
      self.add_write(bin, bytes)
    end

    # Create expression that returns estimated number of elements in the HLL bin.
    #
    # ==== Examples
    # # HLL bin "a" count > 7
    # Exp.gt(HLLExp.getCount(Exp.hllBin("a")), Exp.val(7))
    def self.get_count(bin)
      bytes = Exp.pack(nil, COUNT)
      self.add_read(bin, bytes, Exp::Type::INT)
    end

    # Create expression that returns a HLL object that is the union of all specified HLL objects
    # in the list with the HLL bin.
    #
    # ==== Examples
    # # Union of HLL bins "a" and "b"
    # HLLExp.getUnion(Exp.hllBin("a"), Exp.hllBin("b"))
    #
    # # Union of local HLL list with bin "b"
    # HLLExp.getUnion(Exp.val(list), Exp.hllBin("b"))
    def self.get_union(list, bin)
      bytes = Exp.pack(nil, UNION, list)
      self.add_read(bin, bytes, Exp::Type::HLL)
    end

    # Create expression that returns estimated number of elements that would be contained by
    # the union of these HLL objects.
    #
    # ==== Examples
    # # Union count of HLL bins "a" and "b"
    # HLLExp.getUnionCount(Exp.hllBin("a"), Exp.hllBin("b"))
    #
    # # Union count of local HLL list with bin "b"
    # HLLExp.getUnionCount(Exp.val(list), Exp.hllBin("b"))
    def self.get_union_count(list, bin)
      bytes = Exp.pack(nil, UNION_COUNT, list)
      self.add_read(bin, bytes, Exp::Type::INT)
    end

    # Create expression that returns estimated number of elements that would be contained by
    # the intersection of these HLL objects.
    #
    # ==== Examples
    # # Intersect count of HLL bins "a" and "b"
    # HLLExp.getIntersectCount(Exp.hllBin("a"), Exp.hllBin("b"))
    #
    # # Intersect count of local HLL list with bin "b"
    # HLLExp.getIntersectCount(Exp.val(list), Exp.hllBin("b"))
    def self.get_intersect_count(list, bin)
      bytes = Exp.pack(nil, INTERSECT_COUNT, list)
      self.add_read(bin, bytes, Exp::Type::INT)
    end

    # Create expression that returns estimated similarity of these HLL objects as a
    # 64 bit float.
    #
    # ==== Examples
    # # Similarity of HLL bins "a" and "b" >= 0.75
    # Exp.ge(HLLExp.getSimilarity(Exp.hllBin("a"), Exp.hllBin("b")), Exp.val(0.75))
    def self.get_similarity(list, bin)
      bytes = Exp.pack(nil, SIMILARITY, list)
      self.add_read(bin, bytes, Exp::Type::FLOAT)
    end

    # Create expression that returns index_bit_count and min_hash_bit_count used to create HLL bin
    # in a list of longs. list[0] is index_bit_count and list[1] is min_hash_bit_count.
    #
    # ==== Examples
    # # Bin "a" index_bit_count < 10
    # Exp.lt(
    #   ListExp.getByIndex(ListReturnType.VALUE, Exp::Type::INT, Exp.val(0),
    #     HLLExp.describe(Exp.hllBin("a"))),
    #   Exp.val(10))
    def self.describe(bin)
      bytes = Exp.pack(nil, DESCRIBE)
      self.add_read(bin, bytes, Exp::Type::LIST)
    end

    # Create expression that returns one if HLL bin may contain all items in the list.
    #
    # ==== Examples
    # # Bin "a" may contain value "x"
    # ArrayList<Value> list = new ArrayList<Value>()
    # list.add(Value.get("x"))
    # Exp.eq(HLLExp.mayContain(Exp.val(list), Exp.hllBin("a")), Exp.val(1))
    def self.may_contain(list, bin)
      bytes = Exp.pack(nil, MAY_CONTAIN, list)
      self.add_read(bin, bytes, Exp::Type::INT)
    end

    private

    MODULE = 2
    INIT = 0
    ADD = 1
    COUNT = 50
    UNION = 51
    UNION_COUNT = 52
    INTERSECT_COUNT = 53
    SIMILARITY = 54
    DESCRIBE = 55
    MAY_CONTAIN = 56

    def self.add_write(bin, bytes)
      Exp::Module.new(bin, bytes, Exp::Type::HLL, MODULE | Exp::MODIFY)
    end

    def self.add_read(bin, bytes, ret_type)
      Exp::Module.new(bin, bytes, ret_type, MODULE)
    end
  end
end
