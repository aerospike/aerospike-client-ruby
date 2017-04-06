# encoding: utf-8
# Copyright 2016-2017 Aerospike, Inc.
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
  module TTL

    # Use the default TTL value for the namespace of the record.
    NAMESPACE_DEFAULT = 0

    # Never expire the record.
    NEVER_EXPIRE = -1

    # Update record without changing the record's TTL value.
    # Requires Aerospike Server version 3.10.1 or later.
    DONT_UPDATE = -2

  end
end
