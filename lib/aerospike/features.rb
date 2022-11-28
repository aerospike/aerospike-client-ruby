# frozen_string_literal: true

# Copyright 2014-2020 Aerospike, Inc.
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

module Aerospike

  # Feature flags describing capabilities of the Aerospike server cluster the
  # client is connected to. The list of supported server features can be
  # retrieved by issuing the "features" info command.
  module Features

    # Server supports List Complex Data Type (CDT)
    CDT_LIST = :'cdt-list'

    # Server supports Map Complex Data Type (CDT)
    CDT_MAP = :'cdt-map'

    # Server supports Float data type
    FLOAT = :float

    # Server supports geo-spatial data type and indexing
    GEO = :geo

    # Server requires 'lut=now' in truncate command (AER-5955)
    LUT_NOW = :'lut-now'

    # Server supports the new 'peers' protocol for automatic node discovery
    PEERS = :peers

    # Server supports the 'truncate-namespace' command
    TRUNCATE_NAMESPACE = :'truncate-namespace'

    # Server supports the 'blob-bits' command
    BLOB_BITS = :'blob-bits'

    # Server supports resumable partition scans
    PARTITION_SCAN = :'pscans'

    # Server supports the 'query-show' command to check for the
    # progress of the scans and queries
    QUERY_SHOW = :'query-show'

    # Server supports the batch command for all types of operations, including wrties
    BATCH_ANY = :'batch-any'

    # Server supports resumable partition queries
    PARTITION_QUERY = :'pquery'
  end
end
