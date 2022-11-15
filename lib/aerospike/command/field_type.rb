# Copyright 2014-2020 Aerospike, Inc.
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

  private

  module FieldType

    NAMESPACE  = 0
    TABLE      = 1
    KEY        = 2
    DIGEST_RIPE  = 4
    DIGEST_RIPE_ARRAY     = 6
    TRAN_ID               = 7 # user supplied transaction id, which is simply passed back
    SCAN_OPTIONS          = 8
    SOCKET_TIMEOUT        = 9
    RECORDS_PER_SECOND    = 10
    PID_ARRAY             = 11
    DIGEST_ARRAY          = 12
    MAX_RECORDS           = 13
    BVAL_ARRAY            = 15
    INDEX_NAME            = 21
    INDEX_RANGE           = 22
    INDEX_CONTEXT         = 23
    INDEX_TYPE            = 26
    UDF_PACKAGE_NAME      = 30
    UDF_FUNCTION          = 31
    UDF_ARGLIST           = 32
    UDF_OP                = 33
    QUERY_BINLIST         = 40
    BATCH_INDEX           = 41
    BATCH_INDEX_WITH_SET  = 42
    FILTER_EXP            = 43

  end # module

end # module
