# encoding: utf-8
# Copyright 2016-2017 Aerospike, Inc.
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

      attr_reader :map_op, :arguments, :return_type

      def initialize(op_type, map_op, bin_name, *arguments, return_type: nil)
        @op_type = op_type
        @bin_name = bin_name
        @bin_value = nil
        @map_op = map_op
        @arguments = arguments
        @return_type = return_type
        self
      end

      ##
      # Create set map policy operation.
      # Server sets map policy attributes. Server returns null.
      #
      # The required map policy attributes can be changed after the map is created.
      def self.set_policy(bin_name, policy)
        MapOperation.new(Operation::CDT_MODIFY, SET_TYPE, bin_name, policy.order)
      end

      ##
      # Create map put operation.
      # Server writes key/value item to map bin and returns map size.
      #
      # The map policy dictates the type of map to create when it does not exist.
      # The map policy also specifies the mode used when writing items to the map.
      def self.put(bin_name, key, value, policy: MapPolicy::DEFAULT)
        case policy.write_mode
        when MapWriteMode::UPDATE_ONLY
          # Replace doesn't allow map order because it does not create on non-existing key.
          MapOperation.new(Operation::CDT_MODIFY, REPLACE, bin_name, key, value)
        when MapWriteMode::CREATE_ONLY
          MapOperation.new(Operation::CDT_MODIFY, ADD, bin_name, key, value, policy.order)
        else
          MapOperation.new(Operation::CDT_MODIFY, PUT, bin_name, key, value, policy.order)
        end
      end

      ##
      # Create map put items operation
      # Server writes each map item to map bin and returns map size.
      #
      # The map policy dictates the type of map to create when it does not exist.
      # The map policy also specifies the mode used when writing items to the map.
      def self.put_items(bin_name, values, policy: MapPolicy::DEFAULT)
        case policy.write_mode
        when MapWriteMode::UPDATE_ONLY
          # Replace doesn't allow map order because it does not create on non-existing key.
          MapOperation.new(Operation::CDT_MODIFY, REPLACE_ITEMS, bin_name, values)
        when MapWriteMode::CREATE_ONLY
          MapOperation.new(Operation::CDT_MODIFY, ADD_ITEMS, bin_name, values, policy.order)
        else
          MapOperation.new(Operation::CDT_MODIFY, PUT_ITEMS, bin_name, values, policy.order)
        end
      end

      ##
      # Create map increment operation.
      # Server increments values by incr for all items identified by key and returns final result.
      # Valid only for numbers.
      #
      # The map policy dictates the type of map to create when it does not exist.
      # The map policy also specifies the mode used when writing items to the map.
      def self.increment(bin_name, key, incr, policy: MapPolicy::DEFAULT)
        MapOperation.new(Operation::CDT_MODIFY, INCREMENT, bin_name, key, incr, policy.order)
      end

      ##
      # Create map decrement operation.
      # Server decrements values by decr for all items identified by key and returns final result.
      # Valid only for numbers.
      #
      # The map policy dictates the type of map to create when it does not exist.
      # The map policy also specifies the mode used when writing items to the map.
      def self.decrement(bin_name, key, decr, policy: MapPolicy::DEFAULT)
        MapOperation.new(Operation::CDT_MODIFY, DECREMENT, bin_name, key, decr, policy.order)
      end

      ##
      # Create map clear operation.
      # Server removes all items in map.  Server returns null.
      def self.clear(bin_name)
        MapOperation.new(Operation::CDT_MODIFY, CLEAR, bin_name)
      end

      ##
      # Create map remove operation.
      #
      # Server removes map item identified by key and returns removed data
      # specified by return_type.
      def self.remove_by_key(bin_name, key, return_type: nil)
        MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY, bin_name, key, return_type: return_type)
      end

      ##
      # Create map remove operation.
      #
      # Server removes map items identified by keys.
      #
      # Server returns removed data specified by return_type.
      def self.remove_by_key_list(bin_name, keys, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY_LIST, bin_name, keys, return_type: return_type)
      end

      ##
      # Create map remove operation.
      #
      # Server removes map items identified by keys.
      #
      # Server returns removed data specified by return_type.
      #
      # Deprecated. Use remove_by_key / remove_by_key_list instead.
      def self.remove_keys(bin_name, *keys, return_type: MapReturnType::NONE)
        if keys.length > 1
          remove_by_key_list(bin_name, keys, return_type: return_type)
        else
          remove_by_key(bin_name, keys.first, return_type: return_type)
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
      def self.remove_by_key_range(bin_name, key_begin, key_end = nil, return_type: MapReturnType::NONE)
        if key_end
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY_INTERVAL, bin_name, key_begin, key_end, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY_INTERVAL, bin_name, key_begin, return_type: return_type)
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
      def self.remove_by_key_rel_index_range(bin_name, key, index, count = nil, return_type: nil)
        if count
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY_REL_INDEX_RANGE, bin_name, key, index, count, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY_REL_INDEX_RANGE, bin_name, key, index, return_type: return_type)
        end
      end

      ##
      # Create map remove operation.
      #
      # Server removes map item identified by value.
      #
      # Server returns removed data specified by return_type.
      def self.remove_by_value(bin_name, value, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE, bin_name, value, return_type: return_type)
      end

      ##
      # Create map remove operation.
      #
      # Server removes map items identified by value.
      #
      # Server returns removed data specified by return_type.
      def self.remove_by_value_list(bin_name, values, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_LIST, bin_name, values, return_type: return_type)
      end

      ##
      # Create map remove operation.
      #
      # Server removes map items identified by value.
      #
      # Server returns removed data specified by return_type.
      #
      # Deprecated. Use remove_by_value / remove_by_value_list instead.
      def self.remove_values(bin_name, *values, return_type: MapReturnType::NONE)
        if values.length > 1
          remove_by_value_list(bin_name, values, return_type: return_type)
        else
          remove_by_value(bin_name, values.first, return_type: return_type)
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
      def self.remove_by_value_range(bin_name, value_begin, value_end = nil, return_type: MapReturnType::NONE)
        if value_end
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_INTERVAL, bin_name, value_begin, value_end, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_INTERVAL, bin_name, value_begin, return_type: return_type)
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
      def self.remove_by_value_rel_rank_range(bin_name, value, rank, count = nil, return_type: nil)
        if count
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_REL_RANK_RANGE, bin_name, value, rank, count, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_REL_RANK_RANGE, bin_name, value, rank, return_type: return_type)
        end
      end

      ##
      # Create map remove operation.
      #
      # Server removes map item identified by index.
      #
      # Server returns removed data specified by return_type.
      def self.remove_by_index(bin_name, index, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_INDEX, bin_name, index, return_type: return_type)
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
      def self.remove_by_index_range(bin_name, index, count = nil, return_type: MapReturnType::NONE)
        if count
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_INDEX_RANGE, bin_name, index, count, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_INDEX_RANGE, bin_name, index, return_type: return_type)
        end
      end
      singleton_class.send(:alias_method, :remove_index_range, :remove_by_index_range)

      ##
      # Create map remove operation.
      #
      # Server removes map item identified by rank.
      #
      # Server returns removed data specified by return_type.
      def self.remove_by_rank(bin_name, rank, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_RANK, bin_name, rank, return_type: return_type)
      end

      ##
      # Create map remove operation.
      #
      # Server selects "count" map items starting at specified rank. If "count"
      # is not specified, server removes map items starting at specified rank
      # to the last ranked.
      #
      # Server returns removed data specified by return_type.
      def self.remove_by_rank_range(bin_name, rank, count = nil, return_type: MapReturnType::NONE)
        if count
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_RANK_RANGE, bin_name, rank, count, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_RANK_RANGE, bin_name, rank, return_type: return_type)
        end
      end

      ##
      # Create map size operation.
      # Server returns size of map.
      def self.size(bin_name)
        MapOperation.new(Operation::CDT_READ, SIZE, bin_name)
      end

      ##
      # Create map get by key operation.
      # Server selects map item identified by key and returns selected data specified by return_type.
      def self.get_by_key(bin_name, key, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_KEY, bin_name, key, return_type: return_type)
      end
      singleton_class.send(:alias_method, :get_key, :get_by_key)

      ##
      # Create map get by key list operation.
      #
      # Server selects map items identified by keys.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_key_list(bin_name, keys, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_KEY_LIST, bin_name, keys, return_type: return_type)
      end

      # Create map get by key range operation.
      #
      # Server selects map items identified by key range (key_begin inclusive,
      # key_end exclusive). If key_begin is null, the range is less than
      # key_end. If key_end is null, the range is greater than equal to
      # key_begin.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_key_range(bin_name, key_begin, key_end = nil, return_type: MapReturnType::NONE)
        if key_end
          MapOperation.new(Operation::CDT_READ, GET_BY_KEY_INTERVAL, bin_name, key_begin, key_end, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_KEY_INTERVAL, bin_name, key_begin, return_type: return_type)
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
      def self.get_by_key_rel_index_range(bin_name, key, index, count = nil, return_type: nil)
        if count
          MapOperation.new(Operation::CDT_READ, GET_BY_KEY_REL_INDEX_RANGE, bin_name, key, index, count, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_KEY_REL_INDEX_RANGE, bin_name, key, index, return_type: return_type)
        end
      end

      # Create map get by value operation.
      #
      # Server selects map items identified by value.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_value(bin_name, value, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_VALUE, bin_name, value, return_type: return_type)
      end
      singleton_class.send(:alias_method, :get_value, :get_by_value)

      # Create map get by value list operation.
      #
      # Server selects map items identified by value list.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_value_list(bin_name, values, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_VALUE_LIST, bin_name, values, return_type: return_type)
      end

      # Create map get by value range operation.
      #
      # Server selects map items identified by value range (value_begin
      # inclusive, value_end exclusive). If value_begin is null, the range is
      # less than value_end. If value_end is null, the range is greater than
      # equal to value_begin.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_value_range(bin_name, value_begin, value_end = nil, return_type: MapReturnType::NONE)
        if value_end
          MapOperation.new(Operation::CDT_READ, GET_BY_VALUE_INTERVAL, bin_name, value_begin, value_end, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_VALUE_INTERVAL, bin_name, value_begin, return_type: return_type)
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
      def self.get_by_value_rel_rank_range(bin_name, value, rank, count = nil, return_type: nil)
        if count
          MapOperation.new(Operation::CDT_READ, GET_BY_VALUE_REL_RANK_RANGE, bin_name, value, rank, count, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_VALUE_REL_RANK_RANGE, bin_name, value, rank, return_type: return_type)
        end
      end


      # Create map get by index operation.
      #
      # Server selects map item identified by index.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_index(bin_name, index, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_INDEX, bin_name, index, return_type: return_type)
      end
      singleton_class.send(:alias_method, :get_index, :get_by_index)

      # Create map get by index range operation.
      #
      # Server selects "count" map items starting at specified index. If
      # "count" is not specified, server selects map items starting at
      # specified index to the end of map.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_index_range(bin_name, index, count = nil, return_type: MapReturnType::NONE)
        if count
          MapOperation.new(Operation::CDT_READ, GET_BY_INDEX_RANGE, bin_name, index, count, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_INDEX_RANGE, bin_name, index, return_type: return_type)
        end
      end
      singleton_class.send(:alias_method, :get_index_range, :get_by_index_range)

      # Create map get by rank operation.
      #
      # Server selects map item identified by rank.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_rank(bin_name, rank, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_RANK, bin_name, rank, return_type: return_type)
      end

      # Create map get by rank range operation.
      #
      # Server selects "count" map items starting at specified rank. If "count"
      # is not specified, server selects map items starting at specified rank
      # to the last ranked item.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_rank_range(bin_name, rank, count = nil, return_type: MapReturnType::NONE)
        if count
          MapOperation.new(Operation::CDT_READ, GET_BY_RANK_RANGE, bin_name, rank, count, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_RANK_RANGE, bin_name, rank, return_type: return_type)
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
        Packer.use do |packer|
          packer.write_raw_short(map_op)
          args = arguments.dup
          args.unshift(return_type) if return_type
          if args.length > 0
            packer.write_array_header(args.length)
            args.each do |value|
              Value.of(value).pack(packer)
            end
          end
          bytes = packer.bytes
        end
        BytesValue.new(bytes)
      end

    end

  end
end
