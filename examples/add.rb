# Copyright 2012-2014 Aerospike, Inc.#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require 'rubygems'
require 'aerospike'
require './shared/shared'

include Aerospike
include Shared

def main
  Shared.init
  run_example(Shared.client)
  Shared.logger.info("Example finished successfully.")
end

def run_example(client)
  key = Key.new(Shared.namespace, Shared.set_name, "addkey")

  bin_name = "addbin"

  # Delete record if it already exists.
  client.delete(key, Shared.write_policy)

  # Perform some adds and check results.
  bin = Bin.new(bin_name, 10)
  Shared.logger.info("Initial add will create record.  Initial value is #{bin.value}.")
  client.add(key, [bin], Shared.write_policy)
  bin = Bin.new(bin_name, 5)
  Shared.logger.info("Add #{bin.value} to existing record.")
  client.add(key, [bin], Shared.write_policy)

  record = client.get(key, [bin.name], Shared.policy)

  if record.nil?
    Shared.logger.fatal("Failed to get: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key}")
    exit
  end

  # The value received from the server is an unsigned byte stream.
  # Convert to an integer before comparing with expected.
  received = record.bins[bin.name]
  expected = 15

  if received == expected
    Shared.logger.info("Add successful: ns=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin.name} value=#{received}")
  else
    Shared.logger.fatal("Add mismatch: Expected #{expected}. Received #{received}.")
    exit
  end

  # Demonstrate add and get combined.
  bin = Bin.new(bin_name, 30)
  Shared.logger.info("Add #{bin.value} to existing record.")
  record = client.operate(key, [Operation.add(bin), Operation.get], Shared.write_policy)

  expected = 45
  received = record.bins[bin.name]

  if received == expected
    Shared.logger.info("Add successful: ns=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin.name} value=#{received}")
  else
    Shared.logger.fatal("Add mismatch: Expected #{expected}. Received #{received}.")
    exit
  end
end

main