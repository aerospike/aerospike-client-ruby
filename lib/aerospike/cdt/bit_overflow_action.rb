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

    # BitOverflowAction specifies the action to take when bitwise add/subtract results in overflow/underflow.
    module BitOverflowAction

      ##
      # Fail specifies to fail operation with error.
      FAIL  = 0

      ##
      # SATURATE specifies that in add/subtract overflows/underflows, set to max/min value.
      # Example: MAXINT + 1 = MAXINT
      SATURATE  = 2

      ##
      # Wrap specifies that in add/subtract overflows/underflows, WRAP the value.
      # Example: MAXINT + 1 = -1
      WRAP = 4

      ##
      # Default behavior
      DEFAULT = FAIL

    end
  end
end
