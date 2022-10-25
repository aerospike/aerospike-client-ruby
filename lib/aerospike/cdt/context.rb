# frozen_string_literal: true

# Copyright 2020 Aerospike, Inc.
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
	# Nested CDT context. Identifies the location of nested list/map to apply the operation.
	# for the current level.
	# An array of CTX identifies location of the list/map on multiple
	# levels on nesting.
    class Context

      attr_accessor :id, :value

      def initialize(id, value)
        @id = id
        @value = value
      end

      ##
      # Create list with given type at index offset, given an order and pad.
      def self.list_index_create(index, order, pad)
      Context.new(0x10 | ListOrder.flag(order, pad), index)
      end

	  ##
	  # Lookup list by index offset.
	  # If the index is negative, the resolved index starts backwards from end of list.
	  # If an index is out of bounds, a parameter error will be returned.
	  # Examples:
	  # 0: First item.
	  # 4: Fifth item.
	  # -1: Last item.
	  # -3: Third to last item.
	  def self.list_index(index)
	  	Context.new(0x10, index)
	  end

	  ##
	  # Lookup list by rank.
	  # 0 = smallest value
	  # N = Nth smallest value
	  # -1 = largest value
	  def self.list_rank(rank)
	  	Context.new(0x11, rank)
	  end

	  ##
	  # Lookup list by value.
	  def self.list_value(key)
	  	Context.new(0x13, key)
	  end

	  ##
	  # Lookup map by index offset.
	  # If the index is negative, the resolved index starts backwards from end of list.
	  # If an index is out of bounds, a parameter error will be returned.
	  # Examples:
	  # 0: First item.
	  # 4: Fifth item.
	  # -1: Last item.
	  # -3: Third to last item.
	  def self.map_index(index)
	  	Context.new(0x20, index)
	  end

	  ##
	  # Lookup map by rank.
	  # 0 = smallest value
	  # N = Nth smallest value
	  # -1 = largest value
	  def self.map_rank(rank)
	  	Context.new(0x21, rank)
	  end

	  ##
	  # Lookup map by key.
	  def self.map_key(key)
	  	Context.new(0x22, key)
	  end

      ##
      # Create map with given type at map key.
      def self.map_key_create(key, order)
        Context.new(0x22 | order[:flag], key)
      end

	  ##
	  # Lookup map by value.
	  def self.map_value(key)
	  	Context.new(0x23, key)
	  end

    end
  end
end
