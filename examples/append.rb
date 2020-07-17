# Copyright 2012-2020 Aerospike, Inc.#
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
  key = Key.new(Shared.namespace, Shared.set_name, "appendkey")

  bin_name = "appendbin"

  # Delete record if it already exists.
  client.delete(key, Shared.write_policy)

  bin = Bin.new(bin_name, "Hello")
  Shared.logger.info("Initial append will create record.  Initial value is #{bin.value}.")
  client.append(key, bin, Shared.write_policy)

  bin = Bin.new(bin_name, " World")
  Shared.logger.info("Append \"#{bin.value}\" to existing record.")
  client.append(key, bin, Shared.write_policy)

  record = client.get(key, [bin.name], Shared.policy)

  received = record.bins[bin.name]
  expected = "Hello World"

  if received == expected
    Shared.logger.info("Append successful: ns=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin.name} value=#{received}")
  else
    Shared.logger.fatal("Append mismatch: Expected #{expected}. Received #{received}")
    exit
  end
end

main
