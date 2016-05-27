# Copyright 2012-2014 Aerospike, Inc.#
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
require 'rubygems'
require 'aerospike'
require './shared/shared'

include Aerospike
include Shared

def main
  Shared.init
  run_single_bin_example(Shared.client)
  run_multi_bin_example(Shared.client)
  run_get_header_example(Shared.client)

  Shared.logger.info("Example finished successfully.")
end

# Execute put and get on a server configured as multi-bin.  This is the server default.
def run_multi_bin_example(client)
  key = Key.new(Shared.namespace, Shared.set_name, "putgetkey")
  bin1 = Bin.new("bin1", "value1")
  bin2 = Bin.new("bin2", "value2")

  Shared.logger.info("Put: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin1=#{bin1.name} value1=#{bin1.value} bin2=#{bin2.name} value2=#{bin2.value}")

  client.put(key, [bin1, bin2], Shared.write_policy)

  Shared.logger.info("Get: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key}")

  record = client.get(key, [], Shared.policy)

  if record.nil?
    Shared.logger.fatal("Failed to get: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key}")
    exit
  end

  Shared.validate_bin(key, bin1, record)
  Shared.validate_bin(key, bin2, record)
end

# Execute put and get on a server configured as single-bin.
def run_single_bin_example(client)
  key = Key.new(Shared.namespace, Shared.set_name, "putgetkey")
  bin = Bin.new("", "value")

  Shared.logger.info("Single Put: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} value=#{bin.value}")

  client.put(key, [bin], Shared.write_policy)

  Shared.logger.info("Single Get: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key}")

  record = client.get(key, [], Shared.policy)

  if record.nil?
    Shared.logger.fatal("Failed to get: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key}")
    exit
  end

  Shared.validate_bin(key, bin, record)
end

# Read record header data.
def run_get_header_example(client)
  key = Key.new(Shared.namespace, Shared.set_name, "putgetkey")

  Shared.logger.info("Get record header: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key}")
  record = client.get_header(key, Shared.policy)

  if record.nil?
    Shared.logger.fatal("Failed to get: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key}")
    exit
  end

  # Generation should be greater than zero.  Make sure it's populated.
  if record.generation == 0
    Shared.logger.fatal("Invalid record header: generation=#{record.generation} expiration=#{record.expiration}")
    exit
  end
  Shared.logger.info("Received: generation=#{record.generation} expiration=#{record.expiration}")
end

main