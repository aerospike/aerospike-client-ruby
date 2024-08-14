# encoding: utf-8
# Copyright 2014-2024 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License") you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

# Batch key and read only operations with default policy.
# Used in batch read commands where different bins are needed for each key.

module Aerospike

 # Batch user defined functions.
 class BatchUDF < BatchRecord

    # Optional UDF policy.
    attr_accessor :policy

    # Package or lua module name.
    attr_accessor :package_name

    # Lua function name.
    attr_accessor :function_name

    # Optional arguments to lua function.
    attr_accessor :function_args

    # Wire protocol bytes for function args. For internal use only.
    attr_reader :arg_bytes

    # Constructor using default policy.
    def initialize(key, package_name, function_name, function_args, opt = {})
      super(key, has_write: true)
      @policy = BatchRecord.create_policy(opt, BatchUDFPolicy, DEFAULT_BATCH_UDF_POLICY)
      @package_name = package_name
      @function_name = function_name
      @function_args = ListValue.new(function_args)
      # Do not set arg_bytes here because may not be necessary if batch repeat flag is used.
    end

    # Optimized reference equality check to determine batch wire protocol repeat flag.
    # For internal use only.
    def ==(other) # :nodoc:
        other && other.instance_of?(self.class) &&
          @function_name == other.function_name && @function_args == other.function_args &&
          @package_name == other.package_name && @policy == other.policy
    end

    DEFAULT_BATCH_UDF_POLICY = BatchUDFPolicy.new

    # Return wire protocol size. For internal use only.
    def size # :nodoc:
     size = 6 # gen(2) + exp(4) = 6

     size += @policy&.filter_exp&.size if @policy&.filter_exp

     if @policy&.send_key
       size += @key.user_key.estimate_size + Aerospike::FIELD_HEADER_SIZE + 1
     end
     size += @package_name.bytesize + Aerospike::FIELD_HEADER_SIZE
     size += @function_name.bytesize + Aerospike::FIELD_HEADER_SIZE
     @arg_bytes = @function_args.to_bytes
     size += @arg_bytes.bytesize + Aerospike::FIELD_HEADER_SIZE
     size
    end
 end
end