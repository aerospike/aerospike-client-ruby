# encoding: utf-8
# Copyright 2018 Aerospike, Inc.
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
    module ListWriteFlags

      ##
      # Default. Allow duplicate values and insertion at any index.
      DEFAULT = 0

      ##
      # Only add unique values.
      ADD_UNIQUE = 1

      ##
      # Enforce list boundaries when inserting. Do not allow values to be
      # inserted at index outside current list boundaries.
      INSERT_BOUNED = 2

      ##
      # Do not raise error if a list item fails due to write flag constraints.
      NO_FAIL = 4

      ##
      # Allow other valid list items to be committed if a list item fails due
      # to write flag constraints.
      PARTIAL = 8
    end
  end
end
