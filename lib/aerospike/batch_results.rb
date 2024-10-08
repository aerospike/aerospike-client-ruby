# encoding: utf-8
# Copyright 2014-2024 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License") you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

# Batch key and read only operations with default policy.
# Used in batch read commands where different bins are needed for each key.

module Aerospike

  # Batch record results.
  class BatchResults

    # Record results.
    attr_accessor :records

    # Indicates if all records returned success.
    attr_accessor :status

    # Constructor.
    def intialize(records, status)
      @records = records
      @status = status
    end

  end
end
