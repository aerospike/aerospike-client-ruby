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

module Aerospike

  # Batch delete operation.
  class BatchDelete < BatchRecord
    # Optional delete policy.
    attr_accessor :policy

    # Initialize policy and key.
    def initialize(key, opt = {})
      super(key, has_write: true)
      @policy = BatchRecord.create_policy(opt, BatchDeletePolicy, DEFAULT_BATCH_DELETE_POLICY)
    end

    def ==(other) # :nodoc:
      other && other.instance_of?(self.class) && @policy == other.policy
    end

    DEFAULT_BATCH_DELETE_POLICY = BatchDeletePolicy.new

    # Return wire protocol size. For internal use only.
    def size # :nodoc:
      size = 6 # gen(2) + exp(4) = 6

      size += @policy&.filter_exp&.size if @policy&.filter_exp
      if @policy&.send_key
          size += @key.user_key.estimate_size + Aerospike::FIELD_HEADER_SIZE + 1
      end

      size
    end
  end
end