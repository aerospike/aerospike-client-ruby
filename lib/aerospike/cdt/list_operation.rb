# frozen_string_literal: true

# Copyright 2016-2018 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

module Aerospike
  module CDT

    ##
    # List bin operations. Create list operations used by the Client#operate
    # command. List operations support negative indexing. If the index is
    # negative, the resolved index starts backwards from end of list.
    #
    # Index/Range examples:
    # * Index 0: First item in list.
    # * Index 4: Fifth item in list.
    # * Index -1: Last item in list.
    # * Index -3: Third to last item in list.
    # * Index 1 Count 2: Second and third items in list.
    # * Index -3 Count 3: Last three items in list.
    # * Index -5 Count 4: Range between fifth to last item to second to last
    #   item inclusive.
    #
    # If an index is out of bounds, a parameter error will be returned. If a
    # range is partially out of bounds, the valid part of the range will be
    # returned.

    class ListOperation < Operation

      SET_TYPE = 0
      APPEND = 1
      APPEND_ITEMS = 2
      INSERT = 3
      INSERT_ITEMS = 4
      POP = 5
      POP_RANGE = 6
      REMOVE = 7
      REMOVE_RANGE = 8
      SET = 9
      TRIM = 10
      CLEAR = 11
      INCREMENT = 12
      SORT = 13
      SIZE = 16
      GET = 17
      GET_RANGE = 18
      GET_BY_INDEX = 19
      GET_BY_RANK = 21
      GET_BY_VALUE = 22
      GET_BY_VALUE_LIST = 23
      GET_BY_INDEX_RANGE = 24
      GET_BY_VALUE_INTERVAL = 25
      GET_BY_RANK_RANGE = 26
      GET_BY_VALUE_REL_RANK_RANGE = 27
      REMOVE_BY_INDEX = 32
      REMOVE_BY_RANK = 34
      REMOVE_BY_VALUE = 35
      REMOVE_BY_VALUE_LIST = 36
      REMOVE_BY_INDEX_RANGE = 37
      REMOVE_BY_VALUE_INTERVAL = 38
      REMOVE_BY_RANK_RANGE = 39
      REMOVE_BY_VALUE_REL_RANK_RANGE = 40

      attr_reader :list_op, :arguments, :policy, :return_type

      def initialize(op_type, list_op, bin_name, *arguments, policy: nil, return_type: nil)
        @op_type = op_type
        @bin_name = bin_name
        @bin_value = nil
        @list_op = list_op
        @arguments = arguments
        @policy = policy
        @return_type = return_type
      end

      ##
      #  Create a set list order operation.
      #  Server sets list order.
      #  Server returns null.
      def self.set_order(bin_name, order)
        ListOperation.new(Operation::CDT_MODIFY, SET_TYPE, bin_name, order)
      end

      ##
      #  Create list append operation.
      #  Server appends value(s) to end of the list bin.
      #  Server returns list size.
      def self.append(bin_name, *values, policy: ListPolicy::DEFAULT)
        if values.length > 1
          ListOperation.new(Operation::CDT_MODIFY, APPEND_ITEMS, bin_name, values, policy.order, policy.flags)
        else
          ListOperation.new(Operation::CDT_MODIFY, APPEND, bin_name, values.first, policy.order, policy.flags)
        end
      end

      ##
      #  Create list insert operation.
      #  Server inserts value(s) at the specified index of the list bin.
      #  Server returns list size.
      def self.insert(bin_name, index, *values, policy: nil)
        if values.length > 1
          ListOperation.new(Operation::CDT_MODIFY, INSERT_ITEMS, bin_name, index, values, policy: policy)
        else
          ListOperation.new(Operation::CDT_MODIFY, INSERT, bin_name, index, values.first, policy: policy)
        end
      end

      ##
      # Create list pop operation.
      # Server returns item at specified index and removes item from list bin.
      def self.pop(bin_name, index)
        ListOperation.new(Operation::CDT_MODIFY, POP, bin_name, index)
      end

      ##
      # Create list pop range operation.
      # Server returns "count" items starting at specified index and removes
      # items from list bin. If "count" is not specified, the server returns
      # items starting at the specified index to the end of the list and
      # removes those items from the list bin.
      def self.pop_range(bin_name, index, count=nil)
        if count
          ListOperation.new(Operation::CDT_MODIFY, POP_RANGE, bin_name, index, count)
        else
          ListOperation.new(Operation::CDT_MODIFY, POP_RANGE, bin_name, index)
        end
      end

      ##
      # Create list remove operation.
      # Server removes item at specified index from list bin.
      # Server returns number of items removed.
      def self.remove(bin_name, index)
        ListOperation.new(Operation::CDT_MODIFY, REMOVE, bin_name, index)
      end

      ##
      # Create list remove range operation.
      # Server removes "count" items at specified index from list bin. If
      # "count" is not specified, the server removes all items starting at the
      # specified index to the end of the list.
      # Server returns number of items removed.
      def self.remove_range(bin_name, index, count=nil)
        if count
          ListOperation.new(Operation::CDT_MODIFY, REMOVE_RANGE, bin_name, index, count)
        else
          ListOperation.new(Operation::CDT_MODIFY, REMOVE_RANGE, bin_name, index)
        end
      end

      ##
      # Create list set operation.
      # Server sets item value at specified index in list bin.
      # Server does not return a result by default.
      def self.set(bin_name, index, value, policy: nil)
        ListOperation.new(Operation::CDT_MODIFY, SET, bin_name, index, value, policy: policy)
      end

      ##
      # Create list trim operation.
      # Server removes items in list bin that do not fall into range specified
      # by index and count. If count is not specified, server will keep all
      # items starting at the specified index to the end of the list.
      # Server returns number of items removed.
      def self.trim(bin_name, index, count=nil)
        if count
          ListOperation.new(Operation::CDT_MODIFY, TRIM, bin_name, index, count)
        else
          ListOperation.new(Operation::CDT_MODIFY, TRIM, bin_name, index)
        end
      end

      ##
      # Create list clear operation.
      # Server removes all items in the list bin.
      # Server does not return a result by default.
      def self.clear(bin_name)
        ListOperation.new(Operation::CDT_MODIFY, CLEAR, bin_name)
      end

      ##
      # Create list increment operation.
      # Server increments list[index] by value. If not specified, value defaults to 1.
      # Server returns the value of list[index] after the operation.
      def self.increment(bin_name, index, value = nil)
        if value
          ListOperation.new(Operation::CDT_MODIFY, INCREMENT, bin_name, index, value)
        else
          ListOperation.new(Operation::CDT_MODIFY, INCREMENT, bin_name, index)
        end
      end

      ##
      # Create list sort operation.
      # Server sorts list according to sort_flags.
      # Server does not return a result by default.
      def self.sort(bin_name, sort_flags = ListSortFlags::DEFAULT)
        ListOperation.new(Operation::CDT_MODIFY, SORT, bin_name, sort_flags)
      end

      ##
      # Create list size operation.
      # Server returns size of list.
      def self.size(bin_name)
        ListOperation.new(Operation::CDT_READ, SIZE, bin_name)
      end

      ##
      # Create list get operation.
      # Server returns the item at the specified index in the list bin.
      def self.get(bin_name, index)
        ListOperation.new(Operation::CDT_READ, GET, bin_name, index)
      end

      ##
      # Create list get range operation.
      # Server returns "count" items starting at the specified index in the
      # list bin. If "count" is not specified, the server returns all items
      # starting at the specified index to the end of the list.
      def self.get_range(bin_name, index, count=nil)
        if count
          ListOperation.new(Operation::CDT_READ, GET_RANGE, bin_name, index, count)
        else
          ListOperation.new(Operation::CDT_READ, GET_RANGE, bin_name, index)
        end
      end

      # Create list get by index operation.
      #
      # Server selects list item identified by index.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_index(bin_name, index, return_type: ListReturnType::NONE)
        ListOperation.new(Operation::CDT_READ, GET_BY_INDEX, bin_name, index, return_type: return_type)
      end

      # Create list get by index range operation.
      #
      # Server selects list item identified by index range
      #
      # Server returns selected data specified by return_type.
      def self.get_by_index_range(bin_name, index, count=nil, return_type: ListReturnType::NONE)
        if count
          InvertibleListOp.new(Operation::CDT_READ, GET_BY_INDEX_RANGE, bin_name, index, count, return_type: return_type)
        else
          InvertibleListOp.new(Operation::CDT_READ, GET_BY_INDEX_RANGE, bin_name, index, return_type: return_type)
        end
      end

      # Create list get by rank operation.
      #
      # Server selects list item identified by rank.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_rank(bin_name, rank, return_type: ListReturnType::NONE)
        ListOperation.new(Operation::CDT_READ, GET_BY_RANK, bin_name, rank, return_type: return_type)
      end

      # Create list get by rank range operation.
      #
      # Server selects list item identified by rank range.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_rank_range(bin_name, rank, count=nil, return_type: ListReturnType::NONE)
        if count
          InvertibleListOp.new(Operation::CDT_READ, GET_BY_RANK_RANGE, bin_name, rank, count, return_type: return_type)
        else
          InvertibleListOp.new(Operation::CDT_READ, GET_BY_RANK_RANGE, bin_name, rank, return_type: return_type)
        end
      end

      # Create list get by value operation.
      #
      # Server selects list items identified by value.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_value(bin_name, value, return_type: ListReturnType::NONE)
        InvertibleListOp.new(Operation::CDT_READ, GET_BY_VALUE, bin_name, value, return_type: return_type)
      end

      # Create list get by value range operation.
      #
      # Server selects list items identified by value range (value_begin
      # inclusive, value_end exclusive). If value_begin is null, the range is
      # less than value_end. If value_end is null, the range is greater than
      # equal to value_begin.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_value_range(bin_name, value_begin, value_end = nil, return_type: ListReturnType::NONE)
        if value_end
          InvertibleListOp.new(Operation::CDT_READ, GET_BY_VALUE_INTERVAL, bin_name, value_begin, value_end, return_type: return_type)
        else
          InvertibleListOp.new(Operation::CDT_READ, GET_BY_VALUE_INTERVAL, bin_name, value_begin, return_type: return_type)
        end
      end

      # Create list get by value list operation.
      #
      # Server selects list items identified by values.
      #
      # Server returns selected data specified by return_type.
      def self.get_by_value_list(bin_name, values, return_type: ListReturnType::NONE)
        InvertibleListOp.new(Operation::CDT_READ, GET_BY_VALUE_LIST, bin_name, values, return_type: return_type)
      end

      # Create list get by value relative to rank range list operation.
      #
      # Server selects list items nearest to value and greater, by relative
      # rank with a count limit.
      #
      # Server returns selected data specified by return_type.
      #
      # Examples for ordered list [0, 4, 5, 9, 11, 15]:
      # <ul>
      # <li>(value, rank, count) = [selected items]</li>
      # <li>(5, 0, 2) = [5, 9]</li>
      # <li>(5, 1, 1) = [9]</li>
      # <li>(5, -1, 2) = [4, 5]</li>
      # <li>(3, 0, 1) = [4]</li>
      # <li>(3, 3, 7) = [11, 15]</li>
      # <li>(3, -3, 2) = []</li>
      # </ul>
      #
      # Without count:
      #
      # Examples for ordered list [0, 4, 5, 9, 11, 15]:
      # <ul>
      # <li>(value, rank) = [selected items]</li>
      # <li>(5, 0) = [5, 9, 11, 15]</li>
      # <li>(5, 1) = [9, 11, 15]</li>
      # <li>(5, -1) = [4, 5, 9, 11, 15]</li>
      # <li>(3, 0) = [4, 5, 9, 11, 15]</li>
      # <li>(3, 3) = [11, 15]</li>
      # <li>(3, -3) = [0, 4, 5, 9, 11, 15]</li>
      # </ul>
      def self.get_by_value_rel_rank_range(bin_name, value, rank, count = nil, return_type: ListReturnType::NONE)
        if count
          InvertibleListOp.new(Operation::CDT_READ, GET_BY_VALUE_REL_RANK_RANGE, bin_name, value, rank, count, return_type: return_type)
        else
          InvertibleListOp.new(Operation::CDT_READ, GET_BY_VALUE_REL_RANK_RANGE, bin_name, value, rank, return_type: return_type)
        end
      end

      # Create list remove by index operation.
      #
      # Server removes list item identified by index.
      #
      # Server returns selected data specified by return_type.
      def self.remove_by_index(bin_name, index, return_type: ListReturnType::NONE)
        ListOperation.new(Operation::CDT_MODIFY, REMOVE_BY_INDEX, bin_name, index, return_type: return_type)
      end

      # Create list remove by index range operation.
      #
      # Server removes list item identified by index range
      #
      # Server returns selected data specified by return_type.
      def self.remove_by_index_range(bin_name, index, count=nil, return_type: ListReturnType::NONE)
        if count
          InvertibleListOp.new(Operation::CDT_MODIFY, REMOVE_BY_INDEX_RANGE, bin_name, index, count, return_type: return_type)
        else
          InvertibleListOp.new(Operation::CDT_MODIFY, REMOVE_BY_INDEX_RANGE, bin_name, index, return_type: return_type)
        end
      end

      # Create list remove by rank operation.
      #
      # Server removes list item identified by rank.
      #
      # Server returns selected data specified by return_type.
      def self.remove_by_rank(bin_name, rank, return_type: ListReturnType::NONE)
        ListOperation.new(Operation::CDT_MODIFY, REMOVE_BY_RANK, bin_name, rank, return_type: return_type)
      end

      # Create list remove by rank range operation.
      #
      # Server removes list item identified by rank range.
      #
      # Server returns selected data specified by return_type.
      def self.remove_by_rank_range(bin_name, rank, count=nil, return_type: ListReturnType::NONE)
        if count
          InvertibleListOp.new(Operation::CDT_MODIFY, REMOVE_BY_RANK_RANGE, bin_name, rank, count, return_type: return_type)
        else
          InvertibleListOp.new(Operation::CDT_MODIFY, REMOVE_BY_RANK_RANGE, bin_name, rank, return_type: return_type)
        end
      end

      # Create list remove by value operation.
      #
      # Server removes list items identified by value.
      #
      # Server returns selected data specified by return_type.
      def self.remove_by_value(bin_name, value, return_type: ListReturnType::NONE)
        InvertibleListOp.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE, bin_name, value, return_type: return_type)
      end

      # Create list remove by value range operation.
      #
      # Server removes list items identified by value range (value_begin
      # inclusive, value_end exclusive). If value_begin is null, the range is
      # less than value_end. If value_end is null, the range is greater than
      # equal to value_begin.
      #
      # Server returns selected data specified by return_type.
      def self.remove_by_value_range(bin_name, value_begin, value_end = nil, return_type: ListReturnType::NONE)
        if value_end
          InvertibleListOp.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_INTERVAL, bin_name, value_begin, value_end, return_type: return_type)
        else
          InvertibleListOp.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_INTERVAL, bin_name, value_begin, return_type: return_type)
        end
      end

      # Create list remove by value list operation.
      #
      # Server removes list items identified by values.
      #
      # Server returns selected data specified by return_type.
      def self.remove_by_value_list(bin_name, values, return_type: ListReturnType::NONE)
        InvertibleListOp.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_LIST, bin_name, values, return_type: return_type)
      end

      # Create list remove by value relative to rank range list operation.
      #
      # Server removes list items nearest to value and greater, by relative
      # rank with a count limit.
      #
      # Server returns selected data specified by return_type.
      #
      # Examples for ordered list [0, 4, 5, 9, 11, 15]:
      # <ul>
      # <li>(value, rank, count) = [selected items]</li>
      # <li>(5, 0, 2) = [5, 9]</li>
      # <li>(5, 1, 1) = [9]</li>
      # <li>(5, -1, 2) = [4, 5]</li>
      # <li>(3, 0, 1) = [4]</li>
      # <li>(3, 3, 7) = [11, 15]</li>
      # <li>(3, -3, 2) = []</li>
      # </ul>
      #
      # Without count:
      #
      # Examples for ordered list [0, 4, 5, 9, 11, 15]:
      # <ul>
      # <li>(value, rank) = [selected items]</li>
      # <li>(5, 0) = [5, 9, 11, 15]</li>
      # <li>(5, 1) = [9, 11, 15]</li>
      # <li>(5, -1) = [4, 5, 9, 11, 15]</li>
      # <li>(3, 0) = [4, 5, 9, 11, 15]</li>
      # <li>(3, 3) = [11, 15]</li>
      # <li>(3, -3) = [0, 4, 5, 9, 11, 15]</li>
      # </ul>
      def self.remove_by_value_rel_rank_range(bin_name, value, rank, count = nil, return_type: ListReturnType::NONE)
        if count
          InvertibleListOp.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_REL_RANK_RANGE, bin_name, value, rank, count, return_type: return_type)
        else
          InvertibleListOp.new(Operation::CDT_MODIFY, REMOVE_BY_VALUE_REL_RANK_RANGE, bin_name, value, rank, return_type: return_type)
        end
      end

      def and_return(return_type)
        @return_type = return_type
        @bin_value = nil
        self
      end

      def invert_selection?
        false
      end

      def bin_value
        @bin_value ||= pack_bin_value
      end

      private

      def pack_bin_value
        bytes = nil
        Packer.use do |packer|
          packer.write_raw_short(list_op)
          args = arguments.dup
          if return_type
            rt = return_type
            rt |= ListReturnType::INVERTED if invert_selection?
            args.unshift(rt)
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

    class InvertibleListOp < ListOperation

      ##
      # Invert meaning of list command and return value.
      #
      # For example:
      #
      #     ListOperation.remove_by_index_range(binName, index, count)
      #       .and_return(ListReturnType::VALUE)
      #       .invert_selection
      #
      # When calling invert_selection() on the list operation, the items
      # outside the specified range will be removed and returned.
      def invert_selection
        @invert_selection = !@invert_selection
        self
      end

      def invert_selection?
        !!@invert_selection
      end

    end
  end
end
