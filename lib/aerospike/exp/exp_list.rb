# encoding: utf-8
# Copyright 2014-2022 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License") you may n
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

module Aerospike

  # List expression generator. See {@link Exp}.
  #
  # The bin expression argument in these methods can be a reference to a bin or the
  # result of another expression. Expressions that modify bin values are only used
  # for temporary expression evaluation and are not permanently applied to the bin.
  #
  # List modify expressions the bin's value. This value will be a list except
  # when the list is nested within a map. In that case, a map is returned for the
  # list modify expression.
  #
  # List expressions support negative indexing. If the index is negative, the
  # resolved index starts backwards from end of list. If an index is out of bounds,
  # a parameter error will be returned. If a range is partially out of bounds, the
  # valid part of the range will be returned. Index/Range examples:
  #
  # Index 0: First item in list.
  # Index 4: Fifth item in list.
  # Index -1: Last item in list.
  # Index -3: Third to last item in list.
  # Index 1 Count 2: Second and third items in list.
  # Index -3 Count 3: Last three items in list.
  # Index -5 Count 4: Range between fifth to last item to second to last item inclusive.
  #
  # Nested expressions are supported by optional CTX context arguments.  Example:
  #
  # bin = [[7,9,5],[1,2,3],[6,5,4,1]]
  # Get size of last list.
  # ListExp.size(Exp.listBin("bin"), CTX.listIndex(-1))
  # result = 4
  class Exp::List

    # Create expression that appends value to end of list.
    def self.append(value, bin, ctx: nil, policy: CDT::ListPolicy::DEFAULT)
      bytes = Exp.pack(ctx, APPEND, value, policy.order, policy.flags)
      self.add_write(bin, bytes, ctx)
    end

    # Create expression that appends list items to end of list.
    def self.append_items(list, bin, ctx: nil, policy: CDT::ListPolicy::DEFAULT)
      bytes = Exp.pack(ctx, APPEND_ITEMS, list, policy.order, policy.flags)
      self.add_write(bin, bytes, ctx)
    end

    # Create expression that inserts value to specified index of list.
    def self.insert(index, value, bin, ctx: nil, policy: CDT::ListPolicy::DEFAULT)
      bytes = Exp.pack(ctx, INSERT, index, value, policy.flags)
      self.add_write(bin, bytes, ctx)
    end

    # Create expression that inserts each input list item starting at specified index of list.
    def self.insert_items(index, list, bin, ctx: nil, policy: CDT::ListPolicy::DEFAULT)
      bytes = Exp.pack(ctx, INSERT_ITEMS, index, list, policy.flags)
      self.add_write(bin, bytes, ctx)
    end

    # Create expression that increments list[index] by value.
    # Value expression should resolve to a number.
    def self.increment(index, value, bin, ctx: nil, policy: CDT::ListPolicy::DEFAULT)
      bytes = Exp.pack(ctx, INCREMENT, index, value, policy.order, policy.flags)
      self.add_write(bin, bytes, ctx)
    end

    # Create expression that sets item value at specified index in list.
    def self.set(index, value, bin, ctx: nil, policy: CDT::ListPolicy::DEFAULT)
      bytes = Exp.pack(ctx, SET, index, value, policy.flags)
      self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes all items in list.
    def self.clear(bin, ctx: nil)
      bytes = Exp.pack(ctx, CLEAR)
      self.add_write(bin, bytes, ctx)
    end

    # Create expression that sorts list according to sort_flags.
    #
    # @param sort_flags 	sort flags. See {@link ListSortFlagsend.
    # @param bin			bin or list value expression
    # @param ctx			optional context path for nested CDT
    def self.sort(sort_flags, bin, ctx: nil)
      bytes = Exp.pack(ctx, SORT, sort_flags)
      self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes list items identified by value.
    def self.remove_by_value(value, bin, ctx: nil)
      bytes = Exp.pack(ctx, REMOVE_BY_VALUE, CDT::ListReturnType::NONE, value)
      self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes list items identified by values.
    def self.remove_by_value_list(values, bin, ctx: nil)
      bytes = Exp.pack(ctx, REMOVE_BY_VALUE_LIST, CDT::ListReturnType::NONE, values)
      self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes list items identified by value range (value_begin inclusive, value_end exclusive).
    # If value_begin is nil, the range is less than value_end. If value_end is nil, the range is
    # greater than equal to value_begin.
    def self.remove_by_value_range(value_begin, value_end, bin, ctx: nil)
      bytes = self.pack_range_operation(REMOVE_BY_VALUE_INTERVAL, CDT::ListReturnType::NONE, value_begin, value_end, ctx)
      self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes list items nearest to value and greater by relative rank with a count limit if provided.
    #
    # Examples for ordered list [0,4,5,9,11,15]:
    #
    # (value,rank,count) = [removed items]
    # (5,0,2) = [5,9]
    # (5,1,1) = [9]
    # (5,-1,2) = [4,5]
    # (3,0,1) = [4]
    # (3,3,7) = [11,15]
    # (3,-3,2) = []
    def self.remove_by_value_relative_rank_range(value, rank, bin, ctx: nil, count: nil)
      unless count.nil?
        bytes = Exp.pack(ctx, REMOVE_BY_VALUE_REL_RANK_RANGE, CDT::ListReturnType::NONE, value, rank, count)
      else
        bytes = Exp.pack(ctx, REMOVE_BY_VALUE_REL_RANK_RANGE, CDT::ListReturnType::NONE, value, rank)
      end
      self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes list item identified by index.
    def self.remove_by_index(index, bin, ctx: nil)
      bytes = Exp.pack(ctx, REMOVE_BY_INDEX, CDT::ListReturnType::NONE, index)
      self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes "count" list items starting at specified index.
    def self.remove_by_index_range(index, bin, ctx: nil, count: nil)
      unless count.nil?
        bytes = Exp.pack(ctx, REMOVE_BY_INDEX_RANGE, CDT::ListReturnType::NONE, index, count)
      else
        bytes = Exp.pack(ctx, REMOVE_BY_INDEX_RANGE, CDT::ListReturnType::NONE, index)
      end
      self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes list item identified by rank.
    def self.remove_by_rank(rank, bin, ctx: nil)
      bytes = Exp.pack(ctx, REMOVE_BY_RANK, CDT::ListReturnType::NONE, rank)
      self.add_write(bin, bytes, ctx)
    end

    # Create expression that removes "count" list items starting at specified rank.
    def self.remove_by_rank_range(rank, bin, ctx: nil, count: nil)
      unless count.nil?
        bytes = Exp.pack(ctx, REMOVE_BY_RANK_RANGE, CDT::ListReturnType::NONE, rank, count)
      else
        bytes = Exp.pack(ctx, REMOVE_BY_RANK_RANGE, CDT::ListReturnType::NONE, rank)
      end
      self.add_write(bin, bytes, ctx)
    end

    # Create expression that returns list size.
    #
    # ==== Examples
    # # List bin "a" size > 7
    # Exp.gt(ListExp.size(Exp.listBin("a")), Exp.val(7))
    # end</pre>
    def self.size(bin, ctx: nil)
      bytes = Exp.pack(ctx, SIZE)
      self.add_read(bin, bytes, Exp::Type::INT)
    end

    # Create expression that selects list items identified by value and returns selected
    # data specified by return_type.
    #
    # ==== Examples
    # # List bin "a" contains at least one item == "abc"
    # Exp.gt(
    #   ListExp.getByValue(CDT::ListReturnType::COUNT, Exp.val("abc"), Exp.listBin("a")),
    #   Exp.val(0))
    # end</pre>
    #
    # @param return_type	metadata attributes to return. See {@link CDT::ListReturnTypeend
    # @param value			search expression
    # @param bin			list bin or list value expression
    # @param ctx			optional context path for nested CDT
    def self.get_by_value(return_type, value, bin, ctx: nil)
      bytes = Exp.pack(ctx, GET_BY_VALUE, return_type, value)
      self.add_read(bin, bytes, get_value_type(return_type))
    end

    # Create expression that selects list items identified by value range and returns selected data
    # specified by return_type.
    #
    # ==== Examples
    # # List bin "a" items >= 10 && items < 20
    # ListExp.getByValueRange(CDT::ListReturnType::VALUE, Exp.val(10), Exp.val(20), Exp.listBin("a"))
    # end</pre>
    #
    # @param return_type	metadata attributes to return. See {@link CDT::ListReturnTypeend
    # @param value_begin	begin expression inclusive. If nil, range is less than value_end.
    # @param value_end		end expression exclusive. If nil, range is greater than equal to value_begin.
    # @param bin			bin or list value expression
    # @param ctx			optional context path for nested CDT
    def self.get_by_value_range(return_type, value_begin, value_end, bin, ctx: nil)
      bytes = self.pack_range_operation(GET_BY_VALUE_INTERVAL, return_type, value_begin, value_end, ctx)
      self.add_read(bin, bytes, get_value_type(return_type))
    end

    # Create expression that selects list items identified by values and returns selected data
    # specified by return_type.
    def self.get_by_value_list(return_type, values, bin, ctx: nil)
      bytes = Exp.pack(ctx, GET_BY_VALUE_LIST, return_type, values)
      self.add_read(bin, bytes, get_value_type(return_type))
    end

    # Create expression that selects list items nearest to value and greater by relative rank with a count limit
    # and returns selected data specified by return_type (See {@link CDT::ListReturnTypeend).
    #
    # Examples for ordered list [0,4,5,9,11,15]:
    #
    # (value,rank,count) = [selected items]
    # (5,0,2) = [5,9]
    # (5,1,1) = [9]
    # (5,-1,2) = [4,5]
    # (3,0,1) = [4]
    # (3,3,7) = [11,15]
    # (3,-3,2) = []
    def self.get_by_value_relative_rank_range(return_type, value, rank, bin, ctx: nil, count: nil)
      unless count.nil?
        bytes = Exp.pack(ctx, GET_BY_VALUE_REL_RANK_RANGE, return_type, value, rank, count)
      else
        bytes = Exp.pack(ctx, GET_BY_VALUE_REL_RANK_RANGE, return_type, value, rank)
      end
      self.add_read(bin, bytes, get_value_type(return_type))
    end

    # Create expression that selects list item identified by index and returns
    # selected data specified by return_type.
    #
    # ==== Examples
    # # a[3] == 5
    # Exp.eq(
    #   ListExp.getByIndex(CDT::ListReturnType::VALUE, Exp::Type::INT, Exp.val(3), Exp.listBin("a")),
    #   Exp.val(5))
    # end</pre>
    #
    # @param return_type	metadata attributes to return. See {@link CDT::ListReturnTypeend
    # @param value_type		expected type of value
    # @param index			list index expression
    # @param bin			list bin or list value expression
    # @param ctx			optional context path for nested CDT
    def self.get_by_index(return_type, value_type, index, bin, ctx: nil)
      bytes = Exp.pack(ctx, GET_BY_INDEX, return_type, index)
      self.add_read(bin, bytes, value_type)
    end

    # Create expression that selects list items starting at specified index to the end of list
    # and returns selected data specified by return_type (See {@link CDT::ListReturnTypeend).
    def self.get_by_index_range(return_type, index, bin, ctx: nil)
      bytes = Exp.pack(ctx, GET_BY_INDEX_RANGE, return_type, index)
      self.add_read(bin, bytes, get_value_type(return_type))
    end

    # Create expression that selects "count" list items starting at specified index
    # and returns selected data specified by return_type (See {@link CDT::ListReturnTypeend).
    def self.get_by_index_range(return_type, index, bin, ctx: nil, count: nil)
      unless count.nil?
        bytes = Exp.pack(ctx, GET_BY_INDEX_RANGE, return_type, index, count)
      else
        bytes = Exp.pack(ctx, GET_BY_INDEX_RANGE, return_type, index)
      end
      self.add_read(bin, bytes, get_value_type(return_type))
    end

    # Create expression that selects list item identified by rank and returns selected
    # data specified by return_type.
    #
    # ==== Examples
    # # Player with lowest score.
    # ListExp.getByRank(CDT::ListReturnType::VALUE, Type.STRING, Exp.val(0), Exp.listBin("a"))
    # end</pre>
    #
    # @param return_type	metadata attributes to return. See {@link CDT::ListReturnTypeend
    # @param value_type		expected type of value
    # @param rank 			rank expression
    # @param bin			list bin or list value expression
    # @param ctx			optional context path for nested CDT
    def self.get_by_rank(return_type, value_type, rank, bin, ctx: nil)
      bytes = Exp.pack(ctx, GET_BY_RANK, return_type, rank)
      self.add_read(bin, bytes, value_type)
    end

    # Create expression that selects list items starting at specified rank to the last ranked item
    # and returns selected data specified by return_type (See {@link CDT::ListReturnTypeend).
    def self.get_by_rank_range(return_type, rank, bin, ctx: nil)
      bytes = Exp.pack(ctx, GET_BY_RANK_RANGE, return_type, rank)
      self.add_read(bin, bytes, get_value_type(return_type))
    end

    # Create expression that selects "count" list items starting at specified rank and returns
    # selected data specified by return_type (See {@link CDT::ListReturnTypeend).
    def self.get_by_rank_range(return_type, rank, bin, ctx: nil, count: nil)
      unless count.nil?
        bytes = Exp.pack(ctx, GET_BY_RANK_RANGE, return_type, rank, count)
      else
        bytes = Exp.pack(ctx, GET_BY_RANK_RANGE, return_type, rank)
      end
      self.add_read(bin, bytes, get_value_type(return_type))
    end

    private

    MODULE = 0
    APPEND = 1
    APPEND_ITEMS = 2
    INSERT = 3
    INSERT_ITEMS = 4
    SET = 9
    CLEAR = 11
    INCREMENT = 12
    SORT = 13
    SIZE = 16
    GET_BY_INDEX = 19
    GET_BY_RANK = 21
    GET_BY_VALUE = 22  # GET_ALL_BY_VALUE on server
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

    def self.add_write(bin, bytes, ctx)
      if ctx.to_a.empty?
        ret_type = Exp::Type::LIST
      else
        ret_type = ((ctx[0].id & 0x10) == 0) ? Exp::Type::MAP : Exp::Type::LIST
      end
      Exp::Module.new(bin, bytes, ret_type, MODULE | Exp::MODIFY)
    end

    def self.add_read(bin, bytes, ret_type)
      Exp::Module.new(bin, bytes, ret_type, MODULE)
    end

    def self.get_value_type(return_type)
      if (return_type & ~CDT::ListReturnType::INVERTED) == CDT::ListReturnType::VALUE
        Exp::Type::LIST
      else
        Exp::Type::INT
      end
    end

    def self.pack_range_operation(command, return_type, value_begin, value_end, ctx)
      Packer.use do |packer|
        Exp.pack_ctx(packer, ctx)
        packer.write_array_header(value_end.nil? ? 3 : 4)
        packer.write(command)
        packer.write(return_type)

        unless value_begin.nil?
          if value_begin.is_a?(Exp)
            value_begin.pack(packer)
          else
            Value.of(value_begin).pack(packer)
          end
        else
          packer.write(nil)
        end

        unless value_end.nil?
          if value_end.is_a?(Exp)
            value_end.pack(packer)
          else
            Value.of(value_end).pack(packer)
          end
        end
        packer.bytes
      end
    end
  end # class Exp::List
end # module
