# encoding: utf-8
# Copyright 2016 Aerospike, Inc.
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

      SET_TYPE                  = 64
      ADD                       = 65
      ADD_ITEMS                 = 66
      PUT                       = 67
      PUT_ITEMS                 = 68
      REPLACE                   = 69
      REPLACE_ITEMS             = 70
      INCREMENT                 = 73
      DECREMENT                 = 74
      CLEAR                     = 75
      REMOVE_BY_KEY             = 76
      REMOVE_BY_INDEX           = 77
      REMOVE_BY_RANK            = 79
      REMOVE_BY_KEY_LIST        = 81
      REMOVE_BY_VALUE           = 82
      REMOVE_BY_VALUE_LIST      = 83
      REMOVE_BY_KEY_INTERVAL    = 84
      REMOVE_BY_INDEX_RANGE     = 85
      REMOVE_BY_VALUE_INTERVAL  = 86
      REMOVE_BY_RANK_RANGE      = 87
      SIZE                      = 96
      GET_BY_KEY                = 97
      GET_BY_INDEX              = 98
      GET_BY_RANK               = 100
      GET_BY_VALUE              = 102
      GET_BY_KEY_INTERVAL       = 103
      GET_BY_INDEX_RANGE        = 104
      GET_BY_VALUE_INTERVAL     = 105
      GET_BY_RANK_RANGE         = 106

      attr_reader :map_op, :arguments, :policy, :return_type

      def initialize(op_type, map_op, bin_name, *arguments, policy: nil, return_type: nil)
        @op_type = op_type
        @bin_name = bin_name
        @bin_value = nil
        @map_op = map_op
        @arguments = arguments
        @policy = policy
        @return_type = return_type
        self
      end

      ##
      # Create set map policy operation.
      # Server sets map policy attributes. Server returns null.
      #
      # The required map policy attributes can be changed after the map is created.
      def self.set_policy(bin_name, policy)
        MapOperation.new(Operation::CDT_MODIFY, SET_TYPE, bin_name, policy: policy)
      end

      ##
      # Create map put operation.
      # Server writes key/value item to map bin and returns map size.
      #
      # The map policy dictates the type of map to create when it does not exist.
      # The map policy also specifies the mode used when writing items to the map.
      def self.put(bin_name, key, value, policy: MapPolicy::DEFAULT)
        cmd =
          case policy.write_mode
          when MapWriteMode::UPDATE then PUT
          when MapWriteMode::UPDATE_ONLY then REPLACE
          when MapWriteMode::CREATE_ONLY then ADD
          else PUT
          end
        MapOperation.new(Operation::CDT_MODIFY, cmd, bin_name, key, value, policy: policy)
      end

      ##
      # Create map put items operation
      # Server writes each map item to map bin and returns map size.
      #
      # The map policy dictates the type of map to create when it does not exist.
      # The map policy also specifies the mode used when writing items to the map.
      def self.put_items(bin_name, values, policy: MapPolicy::DEFAULT)
        cmd =
          case policy.write_mode
          when MapWriteMode::UPDATE then PUT_ITEMS
          when MapWriteMode::UPDATE_ONLY then REPLACE_ITEMS
          when MapWriteMode::CREATE_ONLY then ADD_ITEMS
          else PUT_ITEMS
          end
        MapOperation.new(Operation::CDT_MODIFY, cmd, bin_name, values, policy: policy)
      end

      ##
      # Create map increment operation.
      # Server increments values by incr for all items identified by key and returns final result.
      # Valid only for numbers.
      #
      # The map policy dictates the type of map to create when it does not exist.
      # The map policy also specifies the mode used when writing items to the map.
      def self.increment(bin_name, key, incr, policy: MapPolicy::DEFAULT)
        MapOperation.new(Operation::CDT_MODIFY, INCREMENT, bin_name, key, incr, policy: policy)
      end

      ##
      # Create map decrement operation.
      # Server decrements values by decr for all items identified by key and returns final result.
      # Valid only for numbers.
      #
      # The map policy dictates the type of map to create when it does not exist.
      # The map policy also specifies the mode used when writing items to the map.
      def self.decrement(bin_name, key, decr, policy: MapPolicy::DEFAULT)
        MapOperation.new(Operation::CDT_MODIFY, DECREMENT, bin_name, key, decr, policy: policy)
      end

      ##
      # Create map clear operation.
      # Server removes all items in map.  Server returns null.
      def self.clear(bin_name)
        MapOperation.new(Operation::CDT_MODIFY, CLEAR, bin_name)
      end

      ##
      # Create map remove operation.
      # Server removes map item identified by key and returns removed data specified by return_type.
      def self.remove_keys(bin_name, *keys, return_type: MapReturnType::NONE)
        if keys.length > 1
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY_LIST, bin_name, keys, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY, bin_name, keys.first, return_type: return_type)
        end
      end

      ##
      # Create map remove operation.
      # Server removes map items identified by key range (key_begin inclusive, key_end exclusive).
      # If key_begin is null, the range is less than key_end.
      # If key_end is null, the range is greater than equal to key_begin.
      #
      # Server returns removed data specified by return_type.
      def self.remove_key_range(bin_name, key_begin, key_end = nil, return_type: MapReturnType::NONE)
        if key_end
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY_INTERVAL, bin_name, key_begin, key_end, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY_INTERVAL, bin_name, key_begin, return_type: return_type)
        end
      end

      ##
      # Create map remove operation.
      # Server removes map items identified by value and returns removed data specified by return_type.
      def self.remove_values(bin_name, *values, return_type: MapReturnType::NONE)
        if values.length > 1
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_LIST, bin_name, values, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE, bin_name, values.first, return_type: return_type)
        end
      end

      ##
      # Create map remove operation.
      # Server removes map items identified by value range (value_begin inclusive, value_end exclusive).
      # If value_begin is null, the range is less than value_end.
      # If value_end is null, the range is greater than equal to value_begin.
      #
      # Server returns removed data specified by return_type.
      def self.remove_value_range(bin_name, value_begin, value_end = nil, return_type: MapReturnType::NONE)
        if value_end
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_INTERVAL, bin_name, value_begin, value_end, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_INTERVAL, bin_name, value_begin, return_type: return_type)
        end
      end

      ##
      # Create map remove operation.
      # Server removes map item identified by index and returns removed data specified by return_type.
      def self.remove_index(bin_name, index, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_INDEX, bin_name, index, return_type: return_type)
      end

      ##
      # Create map remove operation.
      # Server removes "count" map items starting at specified index and
      # returns removed data specified by return_type. If "count" is not
      # specified, the server selects map items starting at specified index to
      # the end of map.
      def self.remove_index_range(bin_name, index, count = nil, return_type: MapReturnType::NONE)
        if count
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_INDEX_RANGE, bin_name, index, count, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_INDEX_RANGE, bin_name, index, return_type: return_type)
        end
      end

      ##
      # Create map remove operation.
      # Server removes map item identified by rank and returns removed data specified by return_type.
      def self.remove_by_rank(bin_name, rank, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_RANK, bin_name, rank, return_type: return_type)
      end

      ##
      # Create map remove operation.
      # Server selects "count" map items starting at specified rank and returns
      # selected data specified by return_type. If "count" is not specified,
      # server removes map items starting at specified rank to the last ranked.
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
      def self.get_key(bin_name, key, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_KEY, bin_name, key, return_type: return_type)
      end

      # Create map get by key range operation.
      # Server selects map items identified by key range (key_begin inclusive, key_end exclusive).
      # If key_begin is null, the range is less than key_end.
      # If key_end is null, the range is greater than equal to key_begin.
      # <p>
      # Server returns selected data specified by return_type.
      def self.get_key_range(bin_name, key_begin, key_end = nil, return_type: MapReturnType::NONE)
        if key_end
          MapOperation.new(Operation::CDT_READ, GET_BY_KEY_INTERVAL, bin_name, key_begin, key_end, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_KEY_INTERVAL, bin_name, key_begin, return_type: return_type)
        end
      end

      # Create map get by value operation.
      # Server selects map items identified by value and returns selected data specified by return_type.
      def self.get_value(bin_name, value, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_VALUE, bin_name, value, return_type: return_type)
      end

      # Create map get by value range operation.
      # Server selects map items identified by value range (value_begin inclusive, value_end exclusive)
      # If value_begin is null, the range is less than value_end.
      # If value_end is null, the range is greater than equal to value_begin.
      # <p>
      # Server returns selected data specified by return_type.
      def self.get_value_range(bin_name, value_begin, value_end = nil, return_type: MapReturnType::NONE)
        if value_end
          MapOperation.new(Operation::CDT_READ, GET_BY_VALUE_INTERVAL, bin_name, value_begin, value_end, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_VALUE_INTERVAL, bin_name, value_begin, return_type: return_type)
        end
      end

      # Create map get by index operation.
      # Server selects map item identified by index and returns selected data specified by return_type.
      def self.get_index(bin_name, index, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_INDEX, bin_name, index, return_type: return_type)
      end

      # Create map get by index range operation.
      # Server selects "count" map items starting at specified index and
      # returns selected data specified by return_type.  If "count" is not
      # specified, server selects map items starting at specified index to the
      # end of map.
      def self.get_index_range(bin_name, index, count = nil, return_type: MapReturnType::NONE)
        if count
          MapOperation.new(Operation::CDT_READ, GET_BY_INDEX_RANGE, bin_name, index, count, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_INDEX_RANGE, bin_name, index, return_type: return_type)
        end
      end

      # Create map get by rank operation.
      # Server selects map item identified by rank and returns selected data
      # specified by return_type.
      def self.get_by_rank(bin_name, rank, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_RANK, bin_name, rank, return_type: return_type)
      end

      # Create map get by rank range operation.
      # Server selects "count" map items starting at specified rank and returns
      # selected data specified by returnType.  If "count" is not specified,
      # server selects map items starting at specified rank to the last ranked
      # item.
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
          if policy && policy.write_mode != MapWriteMode::UPDATE_ONLY
            args << policy.value
          end
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
