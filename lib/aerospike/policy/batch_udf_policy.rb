# Copyright 2014-2023 Aerospike, Inc.
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

  # Policy attributes used in batch UDF execute commands.
  class BatchUDFPolicy

    attr_accessor :filter_exp, :commit_level, :ttl, :durable_delete, :send_key

    alias expiration ttl
    alias expiration= ttl=

    def initialize(opt={})
      # Optional expression filter. If filter_exp exists and evaluates to false, the specific batch key
      # request is not performed and {BatchRecord#resultCode} is set to
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
      # Default: CommitLevel::COMMIT_ALL
      @commit_level = opt.fetch(:commit_level, CommitLevel::COMMIT_ALL)

      # Record expiration; also known as time-to-live (TTL).
      # Seconds record will live before being removed by the server.
      #
      # Supported values:
      # - `Aerospike::TTL::NEVER_EXPIRE`: Never expire record; requires Aerospike 2
      #    server versions >= 2.7.2 or Aerospike 3 server versions >= 3.1.4. Do
      #    not use for older servers.
      # - `Aerospike::TTL::NAMESPACE_DEFAULT`: Default to namespace configuration
      #    variable "default-ttl" on the server.
      # - `Aerospike::TTL::DONT_UPDATE`: Do not change a record's expiration date
      #   when updating the record. Requires Aerospike server v3.10.1 or later.
      # - Any value > 0: Actual time-to-live in seconds.
      @ttl = opt[:ttl] || opt[:expiration] || 0

      # If the transaction results in a record deletion, leave a tombstone for the record.
      # This prevents deleted records from reappearing after node failures.
      # Valid for Aerospike Server Enterprise Edition only.
      #
      # Default: false (do not tombstone deleted records).
      @durable_delete = opt.fetch(:durable_delete, false)

      # Send user defined key in addition to hash digest.
      # If true, the key will be stored with the record on the server.
      #
      # Default: false (do not send the user defined key)
      @send_key = opt.fetch(:send_key, false)
    end
  end
end