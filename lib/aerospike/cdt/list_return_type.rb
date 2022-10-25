# frozen_string_literal: true

# Copyright 2018 Aerospike, Inc.
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
    module ListReturnType

      ##
      # Do not return a result.
      NONE = 0

      ##
      # Return key index order.
      # 0 = first key
      # N = Nth key
      # -1 = last key
      INDEX = 1

      ##
      # Return reverse key order.
      # 0 = last key
      # -1 = first key
      REVERSE_INDEX = 2

      ##
      # Return value order.
      # 0 = smalles value
      # N = Nth smalles value
      # -1 = largest value
      RANK = 3

      ##
      # Return reverse value order.
      # 0 = largest value
      # N = Nth largest value
      # -1 = smallest values
      REVERSE_RANK = 4

      ##
      # Return count of items selected.
      COUNT = 5

      ##
      # Return value for single key read and value list for range read.
      VALUE = 7

      ##
      # Return true if count > 0.
      EXISTS = 13

      ##
      # :private
      #
      # See ListOperation#invert_selection
      INVERTED = 0x10000

      ##
      # Default return type: NONE
      DEFAULT = NONE

    end
  end
end
