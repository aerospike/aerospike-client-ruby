# encoding: utf-8
# Copyright 2014-2023 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
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


#[:nodoc:]
# DEPRECATED
# TODO: Remove this module on the next major release
module Aerospike

  module Priority

    # The server defines the priority.
    DEFAULT = 0

    # Run the database operation in a background thread.
    LOW = 1

    # Run the database operation at medium priority.
    MEDIUM = 2

    # Run the database operation at the highest priority.
    HIGH = 3

  end # module

end # module
