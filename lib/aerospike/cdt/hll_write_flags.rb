# encoding: utf-8
# Copyright 2020 Aerospike, Inc.
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
    # Map write bit flags.
    # Requires server versions >= 4.3.
    module HLLWriteFlags

      ##
      # Default is Default. Allow create or update.
      DEFAULT = 0

      ##
      # CREATE_ONLY behaves like the following:
      # If the bin already exists, the operation will be denied.
      # If the bin does not exist, a new bin will be created.
      CREATE_ONLY = 1

      ##
      # UPDATE_ONLY behaves like the following:
      # If the bin already exists, the bin will be overwritten.
      # If the bin does not exist, the operation will be denied.
      UPDATE_ONLY = 2

      ##
      # NO_FAIL does not raise error if operation is denied.
      NO_FAIL = 4

      ##
      # ALLOW_FOLD allows the resulting set to be the minimum of provided index bits.
      # Also, allow the usage of less precise HLL algorithms when minHash bits
      # of all participating sets do not match.
      ALLOW_FOLD = 8

    end
  end
end
