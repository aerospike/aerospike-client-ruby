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

    class ListOperation

      APPEND        = 1
      APPEND_ITEMS  = 2
      INSERT        = 3
      INSERT_ITEMS  = 4
      POP           = 5
      POP_RANGE     = 6
      REMOVE        = 7
      REMOVE_RANGE  = 8
      SET           = 9
      TRIM          = 10
      CLEAR         = 11
      SIZE          = 16
      GET           = 17
      GET_RANGE     = 18

      ##
      #  Create list append operation.
      #  Server appends value(s) to end of the list bin.
      #  Server returns list size.
      def self.append(binName, *values)
        if values.length > 1
          create_operation(Operation::CDT_MODIFY, APPEND_ITEMS, binName, values)
        else
          create_operation(Operation::CDT_MODIFY, APPEND, binName, values.first)
        end
      end

      ##
      #  Create list insert operation.
      #  Server inserts value(s) at the specified index of the list bin.
      #  Server returns list size.
      def self.insert(binName, index, *values)
        if values.length > 1
          create_operation(Operation::CDT_MODIFY, INSERT_ITEMS, binName, index, values)
        else
          create_operation(Operation::CDT_MODIFY, INSERT, binName, index, values.first)
        end
      end

      ##
      # Create list pop operation.
      # Server returns item at specified index and removes item from list bin.
      def self.pop(binName, index)
        create_operation(Operation::CDT_MODIFY, POP, binName, index)
      end

      ##
      # Create list pop range operation.
      # Server returns "count" items starting at specified index and removes
      # items from list bin. If "count" is not specified, the server returns
      # items starting at the specified index to the end of the list and
      # removes those items from the list bin.
      def self.pop_range(binName, index, count=nil)
        if count
          create_operation(Operation::CDT_MODIFY, POP_RANGE, binName, index, count)
        else
          create_operation(Operation::CDT_MODIFY, POP_RANGE, binName, index)
        end
      end

      ##
      # Create list remove operation.
      # Server removes item at specified index from list bin.
      # Server returns number of items removed.
      def self.remove(binName, index)
        create_operation(Operation::CDT_MODIFY, REMOVE, binName, index)
      end

      ##
      # Create list remove range operation.
      # Server removes "count" items at specified index from list bin. If
      # "count" is not specified, the server removes all items starting at the
      # specified index to the end of the list.
      # Server returns number of items removed.
      def self.remove_range(binName, index, count=nil)
        if count
          create_operation(Operation::CDT_MODIFY, REMOVE_RANGE, binName, index, count)
        else
          create_operation(Operation::CDT_MODIFY, REMOVE_RANGE, binName, index)
        end
      end

      ##
      # Create list set operation.
      # Server sets item value at specified index in list bin.
      # Server does not return a result by default.
      def self.set(binName, index, value)
        create_operation(Operation::CDT_MODIFY, SET, binName, index, value)
      end

      ##
      # Create list trim operation.
      # Server removes items in list bin that do not fall into range specified
      # by index and count. If count is not specified, server will keep all
      # items starting at the specified index to the end of the list.
      # Server returns number of items removed.
      def self.trim(binName, index, count=nil)
        if count
          create_operation(Operation::CDT_MODIFY, TRIM, binName, index, count)
        else
          create_operation(Operation::CDT_MODIFY, TRIM, binName, index)
        end
      end

      ##
      # Create list clear operation.
      # Server removes all items in the list bin.
      # Server does not return a result by default.
      def self.clear(binName)
        create_operation(Operation::CDT_MODIFY, CLEAR, binName)
      end

      ##
      # Create list size operation.
      # Server returns size of list.
      def self.size(binName)
        create_operation(Operation::CDT_READ, SIZE, binName)
      end

      ##
      # Create list get operation.
      # Server returns the item at the specified index in the list bin.
      def self.get(binName, index)
        create_operation(Operation::CDT_READ, GET, binName, index)
      end

      ##
      # Create list get range operation.
      # Server returns "count" items starting at the specified index in the
      # list bin. If "count" is not specified, the server returns all items
      # starting at the specified index to the end of the list.
      def self.get_range(binName, index, count=nil)
        if count
          create_operation(Operation::CDT_READ, GET_RANGE, binName, index, count)
        else
          create_operation(Operation::CDT_READ, GET_RANGE, binName, index)
        end
      end

      private

      def self.create_operation(type, cdt_type, bin, *args)
        bytes = nil
        Packer.use do |packer|
          packer.write_raw_short(cdt_type)
          if args.length > 0
            packer.write_array_header(args.length)
            args.each do |value|
              Value.of(value).pack(packer)
            end
          end
          bytes = packer.bytes
        end
        return Operation.new(type, bin, BytesValue.new(bytes))
      end

    end
  end
end
