# encoding: utf-8
# Copyright 2014-2016 Aerospike, Inc.
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

require 'aerospike/policy/policy'
require 'aerospike/policy/commit_level'
require 'aerospike/policy/generation_policy'
require 'aerospike/policy/record_exists_action'

module Aerospike

  # Container object for client policy command.
  class WritePolicy < Policy

    attr_accessor :record_exists_action, :generation_policy,
      :generation, :ttl, :send_key, :commit_level,
      :durable_delete

    alias expiration ttl
    alias expiration= ttl=

    def initialize(opt={})
      super(opt)

      # Qualify how to handle writes where the record already exists.
      @record_exists_action = opt[:record_exists_action] || RecordExistsAction::UPDATE

      # Qualify how to handle record writes based on record generation. The default (NONE)
      # indicates that the generation is not used to restrict writes.
      @generation_policy = opt[:gen_policy] || GenerationPolicy::NONE

      # Desired consistency guarantee when committing a transaction on the server. The default
      # (COMMIT_ALL) indicates that the server should wait for master and all replica commits to
      # be successful before returning success to the client.
      @commit_level = opt[:commit_level] || Aerospike::CommitLevel::COMMIT_ALL

      # Expected generation. Generation is the number of times a record has been modified
      # (including creation) on the server. If a write operation is creating a record,
      # the expected generation would be 0
      @generation = opt[:generation] || 0

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

      # Send user defined key in addition to hash digest on a record put.
      # The default is to send the user defined key.
      @send_key = opt[:send_key].nil? ? true : opt[:send_key]

      # If the transaction results in a record deletion, leave a tombstone for
      # the record. This prevents deleted records from reappearing after node
      # failures.
      # Valid for Aerospike Server Enterprise Edition 3.10+ only.
      @durable_delete = opt.fetch(:durable_delete, false)

      self
    end

  end # class

end # module
