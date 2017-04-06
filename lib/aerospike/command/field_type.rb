# encoding: utf-8
# Copyright 2014 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Aerospike

  private

  module FieldType

    NAMESPACE = 0
    TABLE = 1
    KEY = 2
    #BIN = 3
    DIGEST_RIPE = 4
    #GU_TID = 5
    DIGEST_RIPE_ARRAY = 6
    TRAN_ID = 7    # user supplied transaction id, which is simply passed back
    SCAN_OPTIONS = 8
    SCAN_TIMEOUT = 9
    INDEX_NAME = 21
    INDEX_RANGE = 22
    INDEX_FILTER = 23
    INDEX_LIMIT = 24
    INDEX_ORDER_BY = 25
    INDEX_TYPE = 26
    UDF_PACKAGE_NAME = 30
    UDF_FUNCTION = 31
    UDF_ARGLIST = 32
    UDF_OP = 33
    QUERY_BINLIST = 40

  end # module

end # module
