# encoding: utf-8
# Copyright 2014-2017 Aerospike, Inc.
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

  # Pre-defined user roles.
  module Role

    # Manage users and their roles.
    USER_ADMIN = 'user-admin'

    # Manage indicies, user-defined functions and server configuration.
    SYS_ADMIN = 'sys-admin'

    # Allow read, write and UDF transactions with the database.
    READ_WRITE_UDF = "read-write-udf"

    # Allow read and write transactions with the database.
    READ_WRITE = 'read-write'

    # Allow read transactions with the database.
    READ = 'read'

  end # module

end # module