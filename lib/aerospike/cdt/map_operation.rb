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

    ##
    # Unique key map bin operations. Create map operations used by the client operate command.
    # The default unique key map is unordered.
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
    #
    # Nested CDT operations are supported by optional CTX context arguments.  Examples:
    #
    # bin = {key1:{key11:9,key12:4}, key2:{key21:3,key22:5}}
    # Set map value to 11 for map key "key21" inside of map key "key2".
    # MapOperation.put("bin", "key21", 11, ctx: [Context.map_key("key2")])
    # bin result = {key1:{key11:9,key12:4},key2:{key21:11,key22:5}}
    #
    # bin : {key1:{key11:{key111:1},key12:{key121:5}}, key2:{key21:{"key211":7}}}
    # Set map value to 11 in map key "key121" for highest ranked map ("key12") inside of map key "key1".
    # MapOperation.put("bin", "key121", 11, ctx: [Context.map_key("key1"), Context.map_rank(-1)])
    # bin result = {key1:{key11:{key111:1},key12:{key121:11}}, key2:{key21:{"key211":7}}}

    class MapOperation < Operation

      SET_TYPE = 64
      ADD = 65
      ADD_ITEMS = 66
      PUT = 67
      PUT_ITEMS = 68
      REPLACE = 69
      REPLACE_ITEMS = 70
      INCREMENT = 73
      DECREMENT = 74
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
      GET_BY_VALUE = 102
      GET_BY_KEY_INTERVAL = 103
      GET_BY_INDEX_RANGE = 104
      GET_BY_VALUE_INTERVAL = 105
      GET_BY_RANK_RANGE = 106
      GET_BY_KEY_LIST = 107
      GET_BY_VALUE_LIST = 108
      GET_BY_KEY_REL_INDEX_RANGE = 109
      GET_BY_VALUE_REL_RANK_RANGE = 110

      attr_reader :map_op, :arguments, :return_type, :ctx, :flag

      def initialize(op_type, map_op, bin_name, *arguments, ctx: nil, return_type: nil, flag: nil)
        @op_type = op_type
        @bin_name = bin_name
        @bin_value = nil
        @map_op = map_op
        @ctx = ctx
        @flag = flag
        @arguments = arguments
        @return_type = return_type
        self
      end

      ##
      # Creates a map create operation.
      # Server creates map at given context level.
      def self.create(bin_name, order, ctx: nil)
        if !ctx || ctx.length == 0
          # If context not defined, the set order for top-level bin map.
          self.set_policy(MapPolicy.new(order: order, flag: 0), bin_name)
        else
          MapOperation.new(Operation::CDT_MODIFY, SET_TYPE, bin_name, order[:attr], ctx: ctx, flag: order[:flag])
        end
      end

      ##
      # Create set map policy operation.
      # Server sets map policy attributes. Server returns null.
      #
      # The required map policy attributes can be changed after the map is created.
      def self.set_policy(bin_name, policy, ctx: nil)
        MapOperation.new(Operation::CDT_MODIFY, SET_TYPE, bin_name, policy.order[:attr], ctx: ctx)
      end

      ##
      # Create map put operation.
      # Server writes key/value item to map bin and returns map size.
      #
      # The map policy dictates the type of map to create when it does not exist.
      # The map policy also specifies the flags used when writing items to the map.
      def self.put(bin_name, key, value, ctx: nil, policy: MapPolicy::DEFAULT)
        if policy.flags != MapWriteFlags::DEFAULT
          MapOperation.new(Operation::CDT_MODIFY, PUT, bin_name, key, value, policy.order[:attr], policy.flags, ctx: ctx)
        else
          case policy.write_mode
          when MapWriteMode::UPDATE_ONLY
            # Replace doesn't allow map order because it does not create on non-existing key.
            MapOperation.new(Operation::CDT_MODIFY, REPLACE, bin_name, key, value, ctx: ctx)
          when MapWriteMode::CREATE_ONLY
            MapOperation.new(Operation::CDT_MODIFY, ADD, bin_name, key, value, policy.order[:attr], ctx: ctx)
          else
            MapOperation.new(Operation::CDT_MODIFY, PUT, bin_name, key, value, policy.order[:attr], ctx: ctx)
          end
        end
      end

      ##
      # Create map put items operation
      # Server writes each map item to map bin and returns map size.
      #
      # The map policy dictates the type of map to create when it does not exist.
      # The map policy also specifies the flags used when writing items to the map.
      def self.put_items(bin_name, values, ctx: nil, policy: MapPolicy::DEFAULT)
        if policy.flags != MapWriteFlags::DEFAULT
          MapOperation.new(Operation::CDT_MODIFY, PUT_ITEMS, bin_name, values, policy.order[:attr], policy.flags, ctx: ctx)
        else
          case policy.write_mode
          when MapWriteMode::UPDATE_ONLY
            # Replace doesn't allow map order because it does not create on non-existing key.
            MapOperation.new(Operation::CDT_MODIFY, REPLACE_ITEMS, bin_name, values, ctx: ctx)
          when MapWriteMode::CREATE_ONLY
            MapOperation.new(Operation::CDT_MODIFY, ADD_ITEMS, bin_name, values, policy.order[:attr], ctx: ctx)
          else
            MapOperation.new(Operation::CDT_MODIFY, PUT_ITEMS, bin_name, values, policy.order[:attr], ctx: ctx)
          end
        end
      end

      ##
      # Create map increment operation.
      # Server increments values by incr for all items identified by key and returns final result.
      # Valid only for numbers.
      #
      # The map policy dictates the type of map to create when it does not exist.
      # The map policy also specifies the mode used when writing items to the map.
      def self.increment(bin_name, key, incr, ctx: nil, policy: MapPolicy::DEFAULT)
        MapOperation.new(Operation::CDT_MODIFY, INCREMENT, bin_name, key, incr, policy.order[:attr], ctx: ctx)
      end

      ##
      # Create map decrement operation.
      # Server decrements values by decr for all items identified by key and returns final result.
      # Valid only for numbers.
      #
      # The map policy dictates the type of map to create when it does not exist.
      # The map policy also specifies the mode used when writing items to the map.
      def self.decrement(bin_name, key, decr, ctx: nil, policy: MapPolicy::DEFAULT)
        MapOperation.new(Operation::CDT_MODIFY, DECREMENT, bin_name, key, decr, policy.order[:attr], ctx: ctx)
      end

      ##
      # Create map clear operation.
      # Server removes all items in map.  Server returns null.
      def self.clear(bin_name, ctx: nil)
        MapOperation.new(Operation::CDT_MODIFY, CLEAR, bin_name, ctx: ctx)
      end

      ##
      # Create map remove operation.
      #
      # Server removes map item identified by key and returns removed data
      # specified by return_type.
      def self.remove_by_key(bin_name, key, ctx: nil, return_type: nil)
        MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY, bin_name, key, ctx: ctx, return_type: return_type)
      end

      ##
      # Create map remove operation.
      #
      # Server removes map items identified by keys.
      #
      # Server returns removed data specified by return_type.
      def self.remove_by_key_list(bin_name, keys, ctx: nil, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY_LIST, bin_name, keys, ctx: ctx, return_type: return_type)
      end

      ##
      # Create map remove operation.
      #
      # Server removes map items identified by keys.
      #
      # Server returns removed data specified by return_type.
      #
      # Deprecated. Use remove_by_key / remove_by_key_list instead.
      def self.remove_keys(bin_name, *keys, ctx: nil, return_type: MapReturnType::NONE)
        if keys.length > 1
          remove_by_key_list(bin_name, keys, ctx: ctx, return_type: return_type)
        else
          remove_by_key(bin_name, keys.first, ctx: ctx, return_type: return_type)
        end
      end

      ##
      # Create map remove operation.
      #
      # Server removes map items identified by key range (key_begin inclusive, key_end exclusive).
      # If key_begin is null, the range is less than key_end.
      # If key_end is null, the range is greater than equal to key_begin.
      #
      # Server returns removed data specified by return_type.
      def self.remove_by_key_range(bin_name, key_begin, key_end = nil, ctx: nil, return_type: MapReturnType::NONE)
        if key_end
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY_INTERVAL, bin_name, key_begin, key_end, ctx: ctx, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY_INTERVAL, bin_name, key_begin, ctx: ctx, return_type: return_type)
        end
      end
      singleton_class.send(:alias_method, :remove_key_range, :remove_by_key_range)

      ##
      # Create map remove by key relative to index range operation.
      #
      # Server removes map items nearest to key and greater by relative index,
      # with a count limit.
      #
      # Server returns removed data specified by return_type.
      #
      # Examples for map [{0=17},{4=2},{5=15},{9=10}]:
      #
      # * (value, index, count) = [removed items]
      # * (5, 0, 1) = [{5=15}]
      # * (5, 1, 2) = [{9=10}]
      # * (5, -1, 1) = [{4=2}]
      # * (3, 2, 1) = [{9=10}]
      # * (3, -2, 2) = [{0=17}]
      #
      # Without count:
      #
      # * (value, index) = [removed items]
      # * (5, 0) = [{5=15}, {9=10}]
      # * (5, 1) = [{9=10}]
      # * (5, -1) = [{4=2}, {5=15}, {9=10}]
      # * (3, 2) = [{9=10}]
      # * (3, -2) = [{0=17}, {4=2}, {5=15}, {9=10}]
      def self.remove_by_key_rel_index_range(bin_name, key, index, count = nil, ctx: nil, return_type: nil)
        if count
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY_REL_INDEX_RANGE, bin_name, key, index, count, ctx: ctx, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY_REL_INDEX_RANGE, bin_name, key, index, ctx: ctx, return_type: return_type)
        end
      end

      ##
      # Create map remove operation.
      #
      # Server removes map item identified by value.
      #
      # Server returns removed data specified by return_type.
      def self.remove_by_value(bin_name, value, ctx: nil, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE, bin_name, value, ctx: ctx, return_type: return_type)
      end

      ##
      # Create map remove operation.
      #
      # Server removes map items identified by value.
      #
      # Server returns removed data specified by return_type.
      def self.remove_by_value_list(bin_name, values, ctx: nil, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_LIST, bin_name, values, ctx: ctx, return_type: return_type)
      end

      ##
      # Create map remove operation.
      #
      # Server removes map items identified by value.
      #
      # Server returns removed data specified by return_type.
      #
      # Deprecated. Use remove_by_value / remove_by_value_list instead.
      def self.remove_values(bin_name, *values, ctx: nil, return_type: MapReturnType::NONE)
        if values.length > 1
          remove_by_value_list(bin_name, values, ctx: ctx, return_type: return_type)
        else
          remove_by_value(bin_name, values.first, ctx: ctx, return_type: return_type)
        end
      end

      ##
      # Create map remove operation.
      #
      # Server removes map items identified by value range (value_begin
      # inclusive, value_end exclusive). If value_begin is null, the range is
      # less than value_end. If value_end is null, the range is greater than
      # equal to value_begin.
      #
      # Server returns removed data specified by return_type.
      def self.remove_by_value_range(bin_name, value_begin, value_end = nil, ctx: nil, return_type: MapReturnType::NONE)
        if value_end
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_INTERVAL, bin_name, value_begin, value_end, ctx: ctx, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_INTERVAL, bin_name, value_begin, ctx: ctx, return_type: return_type)
        end
      end
      singleton_class.send(:alias_method, :remove_value_range, :remove_by_value_range)

      ##
      # Create map remove by value relative to rank range operation.
      #
      # Server removes "count" map items nearest to value and greater by relative rank.
      # If "count" is not specified, server removes map items nearest to value
      # and greater by relative rank, until the end of the map.
      #
      # Server returns removed data specified by return_type.
      #
      # Examples for map [{4=2},{9=10},{5=15},{0=17}]:
      #
      # * (value, rank, count) = [removed items]
      # * (11, 1, 1) = [{0=17}]
      # * (11, -1, 1) = [{9=10}]
      #
      # Without count:
      #
      # * (value, rank) = [removed items]
      # * (11, 1) = [{0=17}]
      # * (11, -1) = [{9=10}, {5=15}, {0=17}]
      def self.remove_by_value_rel_rank_range(bin_name, value, rank, count = nil, ctx: nil, return_type: nil)
        if count
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_REL_RANK_RANGE, bin_name, value, rank, count, ctx: ctx, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_REL_RANK_RANGE, bin_name, value, rank, ctx: ctx, return_type: return_type)
        end
      end

      ##
      # Create map remove operation.
      #
      # Server removes map item identified by index.
      #
      # Server returns removed data specified by return_type.
      def self.remove_by_index(bin_name, index, ctx: nil, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_INDEX, bin_name, index, ctx: ctx, return_type: return_type)
      end
      singleton_class.send(:alias_method, :remove_index, :remove_by_index)

      ##
      # Create map remove operation.
      #
      # Server removes "count" map items starting at specified index. If
      # "count" is not specified, the server selects map items starting at
      # specified index to the end of map.
      #
      # Server returns removed data specified by return_type.
      def self.remove_by_index_range(bin_name, index, count = nil, ctx: nil, return_type: MapReturnType::NONE)
        if count
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_INDEX_RANGE, bin_name, index, count, ctx: ctx, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_INDEX_RANGE, bin_name, index, ctx: ctx, return_type: return_type)
        end
      end
      singleton_class.send(:alias_method, :remove_index_range, :remove_by_index_range)

      ##
      # Create map remove operation.
      #
      # Server removes map item identified by rank.
      #
      # Server returns removed data specified by return_type.
      def self.remove_by_rank(bin_name, rank, ctx: nil, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_RANK, bin_name, rank, ctx: ctx, return_type: return_type)
      end

      ##
      # Create map remove operation.
      #
      # Server selects "count" map items starting at specified rank. If "count"
      # is not specified, server removes map items starting at specified rank
      # to the last ranked.
      #
      # Server returns removed data specified by return_type.
      def self.remove_by_rank_range(bin_name, rank, count = nil, ctx: nil, return_type: MapReturnType::NONE)
        if count
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_RANK_RANGE, bin_name, rank, count, ctx: ctx, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_RANK_RANGE, bin_name, rank, ctx: ctx, return_type: return_type)
        end
      end

      ##
      # Create map size operation.
      # Server returns size of map.
      def self.size(bin_name, ctx: nil)
        MapOperation.new(Operation::CDT_READ, SIZE, bin_name, ctx: ctx)
      end

      ##
      # Create map get by key operation.
      # Server selects map item identified by key and returns selected data specified by return_type.
      def self.get_by_key(bin_name, key, ctx: nil, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_KEY, bin_name, key, ctx: ctx, return_type: return_type)
      end
      singleton_class.send(:alias_method, :get_key, :get_by_key)

      ##
      # Create map get by key list operation.
      #
      # Server selects map items identified by keys.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_key_list(bin_name, keys, ctx: nil, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_KEY_LIST, bin_name, keys, ctx: ctx, return_type: return_type)
      end

      # Create map get by key range operation.
      #
      # Server selects map items identified by key range (key_begin inclusive,
      # key_end exclusive). If key_begin is null, the range is less than
      # key_end. If key_end is null, the range is greater than equal to
      # key_begin.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_key_range(bin_name, key_begin, key_end = nil, ctx: nil, return_type: MapReturnType::NONE)
        if key_end
          MapOperation.new(Operation::CDT_READ, GET_BY_KEY_INTERVAL, bin_name, key_begin, key_end, ctx: ctx, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_KEY_INTERVAL, bin_name, key_begin, ctx: ctx, return_type: return_type)
        end
      end
      singleton_class.send(:alias_method, :get_key_range, :get_by_key_range)

      ##
      # Create map get by key relative to index range operation.
      #
      # Server selects "count" map items nearest to key and greater by relative
      # index. If "count" is not specified, server selects map items nearest to
      # key and greater by relative index, until the end of the map.
      #
      # Server returns selected data specified by return_type.
      #
      # Examples for map [{0=17},{4=2},{5=15},{9=10}]:
      #
      # * (value, index, count) = [selected items]
      # * (5, 0, 1) = [{5=15}]
      # * (5, 1, 2) = [{9=10}]
      # * (5, -1, 1) = [{4=2}]
      # * (3, 2, 1) = [{9=10}]
      # * (3, -2, 2) = [{0=17}]
      #
      # Without count:
      #
      # * (value, index) = [selected items]
      # * (5, 0) = [{5=15}, {9=10}]
      # * (5, 1) = [{9=10}]
      # * (5, -1) = [{4=2}, {5=15}, {9=10}]
      # * (3, 2) = [{9=10}]
      # * (3, -2) = [{0=17}, {4=2}, {5=15}, {9=10}]
      def self.get_by_key_rel_index_range(bin_name, key, index, count = nil, ctx: nil, return_type: nil)
        if count
          MapOperation.new(Operation::CDT_READ, GET_BY_KEY_REL_INDEX_RANGE, bin_name, key, index, count, ctx: ctx, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_KEY_REL_INDEX_RANGE, bin_name, key, index, ctx: ctx, return_type: return_type)
        end
      end

      # Create map get by value operation.
      #
      # Server selects map items identified by value.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_value(bin_name, value, ctx: nil, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_VALUE, bin_name, value, ctx: ctx, return_type: return_type)
      end
      singleton_class.send(:alias_method, :get_value, :get_by_value)

      # Create map get by value list operation.
      #
      # Server selects map items identified by value list.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_value_list(bin_name, values, ctx: nil, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_VALUE_LIST, bin_name, values, ctx: ctx, return_type: return_type)
      end

      # Create map get by value range operation.
      #
      # Server selects map items identified by value range (value_begin
      # inclusive, value_end exclusive). If value_begin is null, the range is
      # less than value_end. If value_end is null, the range is greater than
      # equal to value_begin.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_value_range(bin_name, value_begin, value_end = nil, ctx: nil, return_type: MapReturnType::NONE)
        if value_end
          MapOperation.new(Operation::CDT_READ, GET_BY_VALUE_INTERVAL, bin_name, value_begin, value_end, ctx: ctx, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_VALUE_INTERVAL, bin_name, value_begin, ctx: ctx, return_type: return_type)
        end
      end
      singleton_class.send(:alias_method, :get_value_range, :get_by_value_range)

      ##
      # Create map get by value relative to rank range operation.
      #
      # Server selects "count" map items nearest to value and greater by relative rank.
      # If "count" is not specified, server selects map items nearest to value
      # and greater by relative rank, until the end of the map.
      #
      # Server returns selected data specified by return_type.
      #
      # Examples for map [{4=2},{9=10},{5=15},{0=17}]:
      #
      # * (value, rank, count) = [selected items]
      # * (11, 1, 1) = [{0=17}]
      # * (11, -1, 1) = [{9=10}]
      #
      # Without count:
      #
      # * (value, rank) = [selected items]
      # * (11, 1) = [{0=17}]
      # * (11, -1) = [{9=10}, {5=15}, {0=17}]
      def self.get_by_value_rel_rank_range(bin_name, value, rank, count = nil, ctx: nil, return_type: nil)
        if count
          MapOperation.new(Operation::CDT_READ, GET_BY_VALUE_REL_RANK_RANGE, bin_name, value, rank, count, ctx: ctx, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_VALUE_REL_RANK_RANGE, bin_name, value, rank, ctx: ctx, return_type: return_type)
        end
      end


      # Create map get by index operation.
      #
      # Server selects map item identified by index.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_index(bin_name, index, ctx: nil, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_INDEX, bin_name, index, ctx: ctx, return_type: return_type)
      end
      singleton_class.send(:alias_method, :get_index, :get_by_index)

      # Create map get by index range operation.
      #
      # Server selects "count" map items starting at specified index. If
      # "count" is not specified, server selects map items starting at
      # specified index to the end of map.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_index_range(bin_name, index, count = nil, ctx: nil, return_type: MapReturnType::NONE)
        if count
          MapOperation.new(Operation::CDT_READ, GET_BY_INDEX_RANGE, bin_name, index, count, ctx: ctx, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_INDEX_RANGE, bin_name, index, ctx: ctx, return_type: return_type)
        end
      end
      singleton_class.send(:alias_method, :get_index_range, :get_by_index_range)

      # Create map get by rank operation.
      #
      # Server selects map item identified by rank.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_rank(bin_name, rank, ctx: nil, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_RANK, bin_name, rank, ctx: ctx, return_type: return_type)
      end

      # Create map get by rank range operation.
      #
      # Server selects "count" map items starting at specified rank. If "count"
      # is not specified, server selects map items starting at specified rank
      # to the last ranked item.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_rank_range(bin_name, rank, count = nil, ctx: nil, return_type: MapReturnType::NONE)
        if count
          MapOperation.new(Operation::CDT_READ, GET_BY_RANK_RANGE, bin_name, rank, count, ctx: ctx, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_RANK_RANGE, bin_name, rank, ctx: ctx, return_type: return_type)
        end
      end

      def and_return(return_type)
        @return_type = return_type
        @bin_value = nil
        self
      end

      def bin_value
        @bin_value ||= pack_bin_value
      end

      private

      def pack_bin_value
        bytes = nil

        args = arguments.dup
        args.unshift(return_type) if return_type

        Packer.use do |packer|
          if @ctx != nil && @ctx.length > 0
            packer.write_array_header(3)
            Value.of(0xff).pack(packer)

            pack_context(packer)

            packer.write_array_header(args.length+1)
            Value.of(@map_op).pack(packer)
          else
            packer.write_raw_short(@map_op)
            if args.length > 0
              packer.write_array_header(args.length)
            end
          end

          if args.length > 0
            args.each do |value|
              Value.of(value).pack(packer)
            end
          end
          bytes = packer.bytes
        end
        BytesValue.new(bytes)
      end

      def pack_context(packer)
        packer.write_array_header(@ctx.length*2)
        if @flag
          (1...@ctx.length).each do |i|
            Value.of(@ctx[i].id).pack(packer)
            Value.of(@ctx[i].value).pack(packer)
          end

          Value.of(@ctx[-1].id | @flag).pack(packer)
          Value.of(@ctx[-1].value).pack(packer)
        else
          @ctx.each do |ctx|
            Value.of(ctx.id).pack(packer)
            Value.of(ctx.value).pack(packer)
          end
        end
      end
    end

  end
end
