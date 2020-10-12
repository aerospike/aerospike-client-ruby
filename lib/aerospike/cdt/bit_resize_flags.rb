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
    # BitResizeFlags specifies the bitwise operation flags for resize.
    module BitResizeFlags

      ##
      # Default specifies the defalt flag.
      DEFAULT = 0

      ##
      # Adds/removes bytes from the beginning instead of the end.
      FROM_FRONT = 1

      ##
      # only allow the byte array size to increase.
      GROW_ONLY = 2

      ##
      # only allow the byte array size to decrease.
      SHRINK_ONLY = 4
    end
  end
end
