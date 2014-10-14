# encoding: utf-8
# Copyright 2014 Aerospike, Inc.
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

module Aerospike

  module GenerationPolicy

    # Do not use record generation to restrict writes.
    NONE = 0

    # Update/delete record if expected generation is equal to server generation. Otherwise, fail.
    EXPECT_GEN_EQUAL = 1

    # Update/delete record if expected generation greater than the server generation. Otherwise, fail.
    # This is useful for restore after backup.
    EXPECT_GEN_GT = 2

    # Create duplicate record if expected generation is not equal to server generation.
    # Duplicates are only created when the server configuration option "allow-versions"
    # is true (default is false).
    DUPLICATE = 3

  end # module

end # module
