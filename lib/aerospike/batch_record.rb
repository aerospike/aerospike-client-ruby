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

  # Batch key and record result.
  class BatchRecord
    # Key.
    attr_reader :key

    # Record result after batch command has completed.  Will be null if record was not found
    # or an error occurred. See {@link BatchRecord#result_code}.
    attr_reader :record

    # Result code for this returned record. See {@link com.aerospike.client.ResultCode}.
    # If not {@link com.aerospike.client.ResultCode#OK}, the record will be null.
    attr_accessor :result_code

    # Is it possible that the write transaction may have completed even though an error
    # occurred for this record. This may be the case when a client error occurs (like timeout)
    # after the command was sent to the server.
    attr_accessor :in_doubt

    # Does this command contain a write operation. For internal use only.
    attr_reader :has_write

    # Constructor.
    def initialize(key, result_code: ResultCode::NO_RESPONSE, in_doubt: false, has_write: false)
      @key = key
      @record = record
      @result_code = result_code
      @in_doubt = in_doubt
      @has_write = has_write
    end

    def self.create_policy(policy, policy_klass, default_policy = nil)
      case policy
      when nil
        default_policy || policy_klass.new
      when policy_klass
        policy
      when Hash
        policy_klass.new(policy)
      else
        raise TypeError, "policy should be a #{policy_klass.name} instance or a Hash"
      end
    end

    # Prepare for upcoming batch call. Reset result fields because this instance might be
    # reused. For internal use only.
    def prepare
      @record = nil
      @result_code = ResultCode::NO_RESPONSE
      @in_doubt = false
    end

    # Set record result. For internal use only.
    def record=(record)
      @record = record
      @result_code = ResultCode::OK
    end

    # Set error result. For internal use only.
    def set_error(result_code, in_doubt)
      @result_code = result_code
      @in_doubt = in_doubt
    end

  end
end
