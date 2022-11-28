# encoding: utf-8
# Copyright 2014-2022 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may no
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

module Aerospike
  # Map expression generator. See {@link Exp}.
  #
  # The bin expression argument in these methods can be a reference to a bin or the
  # result of another expression. Expressions that modify bin values are only used
  # for temporary expression evaluation and are not permanently applied to the bin.
  #
  # Map modify expressions return the bin's value. This value will be a map except
  # when the map is nested within a list. In that case, a list is returned for the
  # map modify expression.
  #
  # All maps maintain an index and a rank.  The index is the item offset from the start of the map,
  # for both unordered and ordered maps.  The rank is the sorted index of the value component.
  # Map supports negative indexing for index and rank.
  #
  # Index examples:
  #
  # Index 0: First item in map.
  # Index 4: Fifth item in map.
  # Index -1: Last item in map.
  # Index -3: Third to last item in map.
  # Index 1 Count 2: Second and third items in map.
  # Index -3 Count 3: Last three items in map.
  # Index -5 Count 4: Range between fifth to last item to second to last item inclusive.
  #
  # Rank examples:
  #
  # Rank 0: Item with lowest value rank in map.
  # Rank 4: Fifth lowest ranked item in map.
  # Rank -1: Item with highest ranked value in map.
  # Rank -3: Item with third highest ranked value in map.
  # Rank 1 Count 2: Second and third lowest ranked items in map.
  # Rank -3 Count 3: Top three ranked items in map.
  #
  # Nested expressions are supported by optional CTX context arguments.  Example:
  #
  # bin = {key1={key11=9,key12=4}, key2={key21=3,key22=5}}
  # Set map value to 11 for map key "key21" inside of map key "key2".
  # Get size of map key2.
  # MapExp.size(mapBin("bin"), CTX.mapKey(Value.get("key2"))
  # result = 2
  class Exp::Map
    # Create expression that writes key/value item to a map bin. The 'bin' expression should either
    # reference an existing map bin or be a expression that returns a map.
    #
    # ==== Examples
    # # Add entry{11,22} to existing map bin.
    # e = Exp.build(MapExp.put(MapPolicy.Default, Exp.val(11), Exp.val(22), Exp.mapBin(binName)))
    # client.operate(key, ExpOperation.write(binName, e, Exp::WriteFlags::DEFAULT))
    #ctx,
    # # Combine entry{11,22} with source map's first index entry and write resulting map to target map bin.
    # e = Exp.build(
    #   MapExp.put(MapPolicy.Default, Exp.val(11), Exp.val(22),
    #     MapExp.getByIndexRange(CDT::MapReturnType::KEY_VALUE, Exp.val(0), Exp.val(1), Exp.mapBin(sourceBinName)))
    #   )
    # client.operate(key, ExpOperation.write(target_bin_name, e, Exp::WriteFlags::DEFAULT))
    def self.put(key, value, bin, ctx: nil, policy: CDT::MapPolicy::DEFAULT)
      Packer.use do |packer|
        if policy.flags != 0
          Exp.pack_ctx(packer, ctx)
          packer.write_array_header(5)
          packer.write(PUT)
          key.pack(packer)
          value.pack(packer)
          packer.write(policy.attributes)
          packer.write(policy.flags)
        else
          if policy.item_command == REPLACE
            # Replace doesn't allow map attributes because it does not create on non-existing key.
            Exp.pack_ctx(packer, ctx)
            packer.write_array_header(3)
            packer.write(policy.item_command)
            key.pack(packer)
            value.pack(packer)
          else
            Exp.pack_ctx(packer, ctx)
            packer.write_array_header(4)
            packer.write(policy.item_command)
            key.pack(packer)
            value.pack(packer)
            packer.write(policy.attributes)
          end
        end
        self.add_write(bin, packer.bytes, ctx)
      end
    end

    # Create expression that writes each map item to a map bin.
    def self.put_items(map, bin, ctx: nil, policy: CDT::MapPolicy::DEFAULT)
      Packer.use do |packer|
        if policy.flags != 0
          Exp.pack_ctx(packer, ctx)
          packer.write_array_header(4)
          packer.write(PUT_ITEMS)
          map.pack(packer)
          packer.write(policy.attributes)
          packer.write(policy.flags)
        else
          if policy.items_command == REPLACE_ITEMS
            # Replace doesn't allow map attributes because it does not create on non-existing key.
            Exp.pack_ctx(packer, ctx)
            packer.write_array_header(2)
            packer.write(policy.items_command)
            map.pack(packer)
          else
            Exp.pack_ctx(packer, ctx)
            packer.write_array_header(3)
            packer.write(policy.items_command)
            map.pack(packer)
            packer.write(policy.attributes)
          end
        end
        self.add_write(bin, packer.bytes, ctx)
      end
    end

    # Create expression that increments values by incr for all items identified by key.
    # Valid only for numbers.
    def self.increment(key, incr, bin, ctx: nil, policy: CDT::MapPolicy::DEFAULT)
      bytes = Exp.pack(ctx, INCREMENT, key, incr, policy.attributes)
      return self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes all items in map.
    def self.clear(bin, ctx: nil)
      bytes = Exp.pack(ctx, CLEAR)
      return self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes map item identified by key.
    def self.remove_by_key(key, bin, ctx: nil)
      bytes = Exp.pack(ctx, REMOVE_BY_KEY, CDT::MapReturnType::NONE, key)
      return self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes map items identified by keys.
    def self.remove_by_key_list(keys, bin, ctx: nil)
      bytes = Exp.pack(ctx, REMOVE_BY_KEY_LIST, CDT::MapReturnType::NONE, keys)
      return self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes map items identified by key range (key_begin inclusive, key_end exclusive).
    # If key_begin is nil, the range is less than key_end.
    # If key_end is nil, the range is greater than equal to key_begin.
    def self.remove_by_key_range(key_begin, key_end, bin, ctx: nil)
      bytes = Exp::List.pack_range_operation(REMOVE_BY_KEY_INTERVAL, CDT::MapReturnType::NONE, key_begin, key_end, ctx)
      return self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes map items nearest to key and greater by index with a count limit if provided.
    #
    # Examples for map [{0=17},{4=2},{5=15},{9=10}]:
    #
    # (value,index,count) = [removed items]
    # (5,0,1) = [{5=15}]
    # (5,1,2) = [{9=10}]
    # (5,-1,1) = [{4=2}]
    # (3,2,1) = [{9=10}]
    # (3,-2,2) = [{0=17}]
    def self.remove_by_key_relative_index_range(key, index, bin, ctx: nil, count: nil)
      unless count.nil?
        bytes = Exp.pack(ctx, REMOVE_BY_KEY_REL_INDEX_RANGE, CDT::MapReturnType::NONE, key, index, count)
      else
        bytes = Exp.pack(ctx, REMOVE_BY_KEY_REL_INDEX_RANGE, CDT::MapReturnType::NONE, key, index)
      end
      return self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes map items identified by value.
    def self.remove_by_value(value, bin, ctx: nil)
      bytes = Exp.pack(ctx, REMOVE_BY_VALUE, CDT::MapReturnType::NONE, value)
      return self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes map items identified by values.
    def self.remove_by_value_list(values, bin, ctx: nil)
      bytes = Exp.pack(ctx, REMOVE_BY_VALUE_LIST, CDT::MapReturnType::NONE, values)
      return self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes map items identified by value range (valueBegin inclusive, valueEnd exclusive).
    # If valueBegin is nil, the range is less than valueEnd.
    # If valueEnd is nil, the range is greater than equal to valueBegin.
    def self.remove_by_value_range(valueBegin, valueEnd, bin, ctx: nil)
      bytes = Exp::List.pack_range_operation(REMOVE_BY_VALUE_INTERVAL, CDT::MapReturnType::NONE, valueBegin, valueEnd, ctx)
      return self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes map items nearest to value and greater by relative rank.
    #
    # Examples for map [{4=2},{9=10},{5=15},{0=17}]:
    #
    # (value,rank) = [removed items]
    # (11,1) = [{0=17}]
    # (11,-1) = [{9=10},{5=15},{0=17}]
    def self.remove_by_value_relative_rank_range(value, rank, bin, ctx: nil)
      bytes = Exp.pack(ctx, REMOVE_BY_VALUE_REL_RANK_RANGE, CDT::MapReturnType::NONE, value, rank)
      return self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes map items nearest to value and greater by relative rank with a count limit.
    #
    # Examples for map [{4=2},{9=10},{5=15},{0=17}]:
    #
    # (value,rank,count) = [removed items]
    # (11,1,1) = [{0=17}]
    # (11,-1,1) = [{9=10}]
    def self.remove_by_value_relative_rank_range(value, rank, count, bin, ctx: nil)
      bytes = Exp.pack(ctx, REMOVE_BY_VALUE_REL_RANK_RANGE, CDT::MapReturnType::NONE, value, rank, count)
      return self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes map item identified by index.
    def self.remove_by_index(index, bin, ctx: nil)
      bytes = Exp.pack(ctx, REMOVE_BY_INDEX, CDT::MapReturnType::NONE, index)
      return self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes "count" map items starting at specified index limited by count if provided.
    def self.remove_by_index_range(index, bin, ctx: nil, count: nil)
      unless count.nil?
        bytes = Exp.pack(ctx, REMOVE_BY_INDEX_RANGE, CDT::MapReturnType::NONE, index, count)
      else
        bytes = Exp.pack(ctx, REMOVE_BY_INDEX_RANGE, CDT::MapReturnType::NONE, index)
      end
      return self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes map item identified by rank.
    def self.remove_by_rank(rank, bin, ctx: nil)
      bytes = Exp.pack(ctx, REMOVE_BY_RANK, CDT::MapReturnType::NONE, rank)
      return self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes "count" map items starting at specified rank. If count is not provided,
    # all items until the last ranked item will be removed
    def self.remove_by_rank_range(rank, bin, ctx: nil, count: nil)
      unless count.nil?
        bytes = Exp.pack(ctx, REMOVE_BY_RANK_RANGE, CDT::MapReturnType::NONE, rank, count)
      else
        bytes = Exp.pack(ctx, REMOVE_BY_RANK_RANGE, CDT::MapReturnType::NONE, rank)
      end
      return self.add_write(bin, bytes, ctx)
    end

    # Create expression that returns list size.
    #
    # ==== Examples
    # # Map bin "a" size > 7
    # Exp.gt(MapExp.size(mapBin("a")), Exp.val(7))
    def self.size(bin, ctx: nil)
      bytes = Exp.pack(ctx, SIZE)
      return self.add_read(bin, bytes, Exp::Type::INT)
    end

    # Create expression that selects map item identified by key and returns selected data
    # specified by return_type.
    #
    # ==== Examples
    # # Map bin "a" contains key "B"
    # Exp.gt(
    #   MapExp.getByKey(CDT::MapReturnType::COUNT, Exp::Type::INT, Exp.val("B"), Exp.mapBin("a")),
    #   Exp.val(0))
    #
    # @param return_type	metadata attributes to return. See {@link MapReturnType}
    # @param value_type		expected type of return value
    # @param key			map key expression
    # @param bin			bin or map value expression
    # @param ctx			optional context path for nested CDT
    def self.get_by_key(return_type, value_type, key, bin, ctx: nil)
      bytes = Exp.pack(ctx, GET_BY_KEY, return_type, key)
      return self.add_read(bin, bytes, value_type)
    end

    # Create expression that selects map items identified by key range (key_begin inclusive, key_end exclusive).
    # If key_begin is nil, the range is less than key_end.
    # If key_end is nil, the range is greater than equal to key_begin.
    #
    # Expression returns selected data specified by return_type (See {@link MapReturnType}).
    def self.get_by_key_range(return_type, key_begin, key_end, bin, ctx: nil)
      bytes = Exp::List.pack_range_operation(GET_BY_KEY_INTERVAL, return_type, key_begin, key_end, ctx)
      return self.add_read(bin, bytes, get_value_type(return_type))
    end

    # Create expression that selects map items identified by keys and returns selected data specified by
    # return_type (See {@link MapReturnType}).
    def self.get_by_key_list(return_type, keys, bin, ctx: nil)
      bytes = Exp.pack(ctx, GET_BY_KEY_LIST, return_type, keys)
      return self.add_read(bin, bytes, get_value_type(return_type))
    end

    # Create expression that selects map items nearest to key and greater by index with a coun.
    # Expression returns selected data specified by return_type (See {@link MapReturnType}).
    #
    # Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
    #
    # (value,index) = [selected items]
    # (5,0) = [{5=15},{9=10}]
    # (5,1) = [{9=10}]
    # (5,-1) = [{4=2},{5=15},{9=10}]
    # (3,2) = [{9=10}]
    # (3,-2) = [{0=17},{4=2},{5=15},{9=10}]
    def self.get_by_key_relative_index_range(return_type, key, index, bin, ctx: nil)
      bytes = Exp.pack(ctx, GET_BY_KEY_REL_INDEX_RANGE, return_type, key, index)
      return self.add_read(bin, bytes, get_value_type(return_type))
    end

    # Create expression that selects map items nearest to key and greater by index with a count limit if provided.
    # Expression returns selected data specified by return_type (See {@link MapReturnType}).
    #
    # Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
    #
    # (value,index,count) = [selected items]
    # (5,0,1) = [{5=15}]
    # (5,1,2) = [{9=10}]
    # (5,-1,1) = [{4=2}]
    # (3,2,1) = [{9=10}]
    # (3,-2,2) = [{0=17}]
    def self.get_by_key_relative_index_range(return_type, key, index, bin, ctx: nil, count: nil)
      unless count.nil?
        bytes = Exp.pack(ctx, GET_BY_KEY_REL_INDEX_RANGE, return_type, key, index, count)
      else
        bytes = Exp.pack(ctx, GET_BY_KEY_REL_INDEX_RANGE, return_type, key, index)
      end
      return self.add_read(bin, bytes, get_value_type(return_type))
    end

    # Create expression that selects map items identified by value and returns selected data
    # specified by return_type.
    #
    # ==== Examples
    # # Map bin "a" contains value "BBB"
    # Exp.gt(
    #   MapExp.getByValue(CDT::MapReturnType::COUNT, Exp.val("BBB"), Exp.mapBin("a")),
    #   Exp.val(0))
    #
    # @param return_type	metadata attributes to return. See {@link MapReturnType}
    # @param value			value expression
    # @param bin			bin or map value expression
    # @param ctx			optional context path for nested CDT
    def self.get_by_value(return_type, value, bin, ctx: nil)
      bytes = Exp.pack(ctx, GET_BY_VALUE, return_type, value)
      return self.add_read(bin, bytes, get_value_type(return_type))
    end

    # Create expression that selects map items identified by value range (valueBegin inclusive, valueEnd exclusive)
    # If valueBegin is nil, the range is less than valueEnd.
    # If valueEnd is nil, the range is greater than equal to valueBegin.
    #
    # Expression returns selected data specified by return_type (See {@link MapReturnType}).
    def self.get_by_value_range(return_type, valueBegin, valueEnd, bin, ctx: nil)
      bytes = Exp::List.pack_range_operation(GET_BY_VALUE_INTERVAL, return_type, valueBegin, valueEnd, ctx)
      return self.add_read(bin, bytes, get_value_type(return_type))
    end

    # Create expression that selects map items identified by values and returns selected data specified by
    # return_type (See {@link MapReturnType}).
    def self.get_by_value_list(return_type, values, bin, ctx: nil)
      bytes = Exp.pack(ctx, GET_BY_VALUE_LIST, return_type, values)
      return self.add_read(bin, bytes, get_value_type(return_type))
    end

    # Create expression that selects map items nearest to value and greater by relative rank (with a count limit if passed).
    # Expression returns selected data specified by return_type (See {@link MapReturnType}).
    #
    # Examples for map [{4=2},{9=10},{5=15},{0=17}]:
    #
    # (value,rank) = [selected items]
    # (11,1) = [{0=17}]
    # (11,-1) = [{9=10},{5=15},{0=17}]
    def self.get_by_value_relative_rank_range(return_type, value, rank, bin, ctx: nil, count: nil)
      unless count.nil?
        bytes = Exp.pack(ctx, GET_BY_VALUE_REL_RANK_RANGE, return_type, value, rank, count)
      else
        bytes = Exp.pack(ctx, GET_BY_VALUE_REL_RANK_RANGE, return_type, value, rank)
      end
      return self.add_read(bin, bytes, get_value_type(return_type))
    end

    # Create expression that selects map item identified by index and returns selected data specified by
    # return_type (See {@link MapReturnType}).
    def self.get_by_index(return_type, value_type, index, bin, ctx: nil)
      bytes = Exp.pack(ctx, GET_BY_INDEX, return_type, index)
      return self.add_read(bin, bytes, value_type)
    end

    # Create expression that selects map items starting at specified index to the end of map and returns selected
    # data specified by return_type (See {@link MapReturnType}) limited by count if provided.
    def self.get_by_index_range(return_type, index, bin, ctx: nil, count: nil)
      unless count.nil?
        bytes = Exp.pack(ctx, GET_BY_INDEX_RANGE, return_type, index, count)
      else
        bytes = Exp.pack(ctx, GET_BY_INDEX_RANGE, return_type, index)
      end
      return self.add_read(bin, bytes, get_value_type(return_type))
    end

    # Create expression that selects map item identified by rank and returns selected data specified by
    # return_type (See {@link MapReturnType}).
    def self.get_by_rank(return_type, value_type, rank, bin, ctx: nil)
      bytes = Exp.pack(ctx, GET_BY_RANK, return_type, rank)
      return self.add_read(bin, bytes, value_type)
    end

    # Create expression that selects map items starting at specified rank to the last ranked item and
    # returns selected data specified by return_type (See {@link MapReturnType}).
    def self.get_by_rank_range(return_type, rank, bin, ctx: nil, count: nil)
      unless count.nil?
        bytes = Exp.pack(ctx, GET_BY_RANK_RANGE, return_type, rank, count)
      else
        bytes = Exp.pack(ctx, GET_BY_RANK_RANGE, return_type, rank)
      end
      return self.add_read(bin, bytes, get_value_type(return_type))
    end

    private

    MODULE = 0
    PUT = 67
    PUT_ITEMS = 68
    REPLACE = 69
    REPLACE_ITEMS = 70
    INCREMENT = 73
    CLEAR = 75
    REMOVE_BY_KEY = 76
    REMOVE_BY_INDEX = 77
    REMOVE_BY_RANK = 79
    REMOVE_BY_KEY_LIST = 81
    REMOVE_BY_VALUE = 82
    REMOVE_BY_VALUE_LIST = 83
    REMOVE_BY_KEY_INTERVAL = 84
    REMOVE_BY_INDEX_RANGE = 85
    REMOVE_BY_VALUE_INTERVAL = 86
    REMOVE_BY_RANK_RANGE = 87
    REMOVE_BY_KEY_REL_INDEX_RANGE = 88
    REMOVE_BY_VALUE_REL_RANK_RANGE = 89
    SIZE = 96
    GET_BY_KEY = 97
    GET_BY_INDEX = 98
    GET_BY_RANK = 100
    GET_BY_VALUE = 102;  # GET_ALL_BY_VALUE on server
    GET_BY_KEY_INTERVAL = 103
    GET_BY_INDEX_RANGE = 104
    GET_BY_VALUE_INTERVAL = 105
    GET_BY_RANK_RANGE = 106
    GET_BY_KEY_LIST = 107
    GET_BY_VALUE_LIST = 108
    GET_BY_KEY_REL_INDEX_RANGE = 109
    GET_BY_VALUE_REL_RANK_RANGE = 110

    def self.add_write(bin, bytes, ctx)
      if ctx.to_a.empty?
        ret_type = Exp::Type::MAP
      else
        ret_type = ((ctx[0].id & 0x10) == 0) ? Exp::Type::MAP : Exp::Type::LIST
      end
      Exp::Module.new(bin, bytes, ret_type, MODULE | Exp::MODIFY)
    end

    def self.add_read(bin, bytes, ret_type)
      Exp::Module.new(bin, bytes, ret_type, MODULE)
    end

    def self.get_value_type(return_type)
      t = return_type & ~CDT::MapReturnType::INVERTED

      if t <= CDT::MapReturnType::COUNT
        return Exp::Type::INT
      end

      if t == CDT::MapReturnType::KEY_VALUE
        return Exp::Type::MAP
      end
      return Exp::Type::LIST
    end
  end # class MapExp
end # module Aerospike
