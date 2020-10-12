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
    # BitWriteFlags specify bitwise operation policy write flags.
    module BitWriteFlags

    ##
    # Default allows create or update.
    DEFAULT = 0

    ##
    # If the bin already exists, the operation will be denied.
    # If the bin does not exist, a new bin will be created.
    CREATE_ONLY = 1

    ##
    # If the bin already exists, the bin will be overwritten.
    # If the bin does not exist, the operation will be denied.
    UPDATE_ONLY = 2

    ##
    # Will not raise error if operation is denied.
    NO_FAIL = 4

    ##
    # Partial allows other valid operations to be committed if this operations is
    # denied due to flag constraints.
    PARTIAL = 8
    end
  end
end
