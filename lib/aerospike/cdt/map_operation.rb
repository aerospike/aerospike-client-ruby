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

      def self.set_map_policy(binName, policy)
        MapOperation.new(Operation::CDT_MODIFY, SET_TYPE, binName, policy: policy)
      end

      def self.put(binName, key, value, policy: MapPolicy::DEFAULT)
        cmd =
          case policy.write_mode
          when MapWriteMode::UPDATE then PUT
          when MapWriteMode::UPDATE_ONLY then REPLACE
          when MapWriteMode::CREATE_ONLY then ADD
          else PUT
          end
        MapOperation.new(Operation::CDT_MODIFY, cmd, binName, key, value, policy: policy)
      end

      def self.put_items(binName, values, policy: MapPolicy::DEFAULT)
        cmd =
          case policy.write_mode
          when MapWriteMode::UPDATE then PUT_ITEMS
          when MapWriteMode::UPDATE_ONLY then REPLACE_ITEMS
          when MapWriteMode::CREATE_ONLY then ADD_ITEMS
          else PUT_ITEMS
          end
        MapOperation.new(Operation::CDT_MODIFY, cmd, binName, values, policy: policy)
      end

      def self.increment(binName, key, value, policy: MapPolicy::DEFAULT)
        MapOperation.new(Operation::CDT_MODIFY, INCREMENT, binName, key, value, policy: policy)
      end

      def self.decrement(binName, key, value, policy: MapPolicy::DEFAULT)
        MapOperation.new(Operation::CDT_MODIFY, DECREMENT, binName, key, value, policy: policy)
      end

      def self.clear(binName)
        MapOperation.new(Operation::CDT_MODIFY, CLEAR, binName)
      end

      def self.remove_keys(binName, *keys, return_type: MapReturnType::NONE)
        if keys.length > 1
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY_LIST, binName, keys, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY, binName, keys.first, return_type: return_type)
        end
      end

      def self.remove_key_range(bin_name, key_begin, key_end = nil, return_type: MapReturnType::NONE)
        if key_end
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY_INTERVAL, bin_name, key_begin, key_end, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_KEY_INTERVAL, bin_name, key_begin, return_type: return_type)
        end
      end

      def self.remove_values(binName, *values, return_type: MapReturnType::NONE)
        if values.length > 1
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_LIST, binName, values, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE, binName, values.first, return_type: return_type)
        end
      end

      def self.remove_value_range(bin_name, value_begin, value_end = nil, return_type: MapReturnType::NONE)
        if value_end
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_INTERVAL, bin_name, value_begin, value_end, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_INTERVAL, bin_name, value_begin, return_type: return_type)
        end
      end

      def self.remove_index(binName, index, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_INDEX, binName, index, return_type: return_type)
      end

      def self.remove_index_range(binName, index, count = nil, return_type: MapReturnType::NONE)
        if count
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_INDEX_RANGE, binName, index, count, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_INDEX_RANGE, binName, index, return_type: return_type)
        end
      end

      def self.remove_by_rank(binName, rank, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_RANK, binName, rank, return_type: return_type)
      end

      def self.remove_by_rank_range(binName, rank, count = nil, return_type: MapReturnType::NONE)
        if count
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_RANK_RANGE, binName, rank, count, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_MODIFY, REMOVE_BY_RANK_RANGE, binName, rank, return_type: return_type)
        end
      end

      def self.size(binName)
        MapOperation.new(Operation::CDT_READ, SIZE, binName)
      end

      def self.get_key(binName, key, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_KEY, binName, key, return_type: return_type)
      end

      def self.get_key_range(bin_name, key_begin, key_end = nil, return_type: MapReturnType::NONE)
        if key_end
          MapOperation.new(Operation::CDT_READ, GET_BY_KEY_INTERVAL, bin_name, key_begin, key_end, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_KEY_INTERVAL, bin_name, key_begin, return_type: return_type)
        end
      end

      def self.get_value(binName, value, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_VALUE, binName, value, return_type: return_type)
      end

      def self.get_value_range(bin_name, value_begin, value_end = nil, return_type: MapReturnType::NONE)
        if value_end
          MapOperation.new(Operation::CDT_READ, GET_BY_VALUE_INTERVAL, bin_name, value_begin, value_end, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_VALUE_INTERVAL, bin_name, value_begin, return_type: return_type)
        end
      end

      def self.get_index(binName, index, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_INDEX, binName, index, return_type: return_type)
      end

      def self.get_index_range(binName, index, count = nil, return_type: MapReturnType::NONE)
        if count
          MapOperation.new(Operation::CDT_READ, GET_BY_INDEX_RANGE, binName, index, count, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_INDEX_RANGE, binName, index, return_type: return_type)
        end
      end

      def self.get_by_rank(binName, rank, return_type: MapReturnType::NONE)
        MapOperation.new(Operation::CDT_READ, GET_BY_RANK, binName, rank, return_type: return_type)
      end

      def self.get_by_rank_range(binName, rank, count = nil, return_type: MapReturnType::NONE)
        if count
          MapOperation.new(Operation::CDT_READ, GET_BY_RANK_RANGE, binName, rank, count, return_type: return_type)
        else
          MapOperation.new(Operation::CDT_READ, GET_BY_RANK_RANGE, binName, rank, return_type: return_type)
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
