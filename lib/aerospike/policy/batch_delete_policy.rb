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

  # Policy attributes used in batch delete commands.
  class BatchDeletePolicy
    attr_accessor :filter_exp, :commit_level, :generation_policy, :generation, :durable_delete, :send_key

    def initialize(opt = {})
      # Optional expression filter. If filter_exp exists and evaluates to false, the specific batch key
      # request is not performed and {BatchRecord#result_code} is set to
      # {ResultCode#FILTERED_OUT}.
      #
      # If exists, this filter overrides the batch parent filter {Policy#filter_exp}
      # for the specific key in batch commands that allow a different policy per key.
      # Otherwise, this filter is ignored.
      #
      # Default: nil
      @filter_exp = opt[:filter_exp]

      # Desired consistency guarantee when committing a transaction on the server. The default
      # (COMMIT_ALL) indicates that the server should wait for master and all replica commits to
      # be successful before returning success to the client.
      #
      # Default: CommitLevel.COMMIT_ALL
      @commit_level = opt[:commit_level] || CommitLevel::COMMIT_ALL

      # Qualify how to handle record deletes based on record generation. The default (NONE)
      # indicates that the generation is not used to restrict deletes.
      #
      # Default: GenerationPolicy.NONE
      @generation_policy = opt[:generation_policy] || GenerationPolicy::NONE

      # Expected generation. Generation is the number of times a record has been modified
      # (including creation) on the server. This field is only relevant when generationPolicy
      # is not NONE.
      #
      # Default: 0
      @generation = opt[:generation] || 0

      # If the transaction results in a record deletion, leave a tombstone for the record.
      # This prevents deleted records from reappearing after node failures.
      # Valid for Aerospike Server Enterprise Edition only.
      #
      # Default: false (do not tombstone deleted records).
      @durable_delete = opt[:durable_delete] || false

      # Send user defined key in addition to hash digest.
      # If true, the key will be stored with the tombstone record on the server.
      #
      # Default: false (do not send the user defined key)
      @send_key = opt[:send_key] || false

      self
    end
  end
end