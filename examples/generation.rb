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
  key = Key.new(Shared.namespace, Shared.set_name, "genkey")
  bin_name = "genbin"

  # Delete record if it already exists.
  client.delete(key, Shared.write_policy)

  # Set some values for the same record.
  bin = Bin.new(bin_name, "genvalue1")
  Shared.logger.info("Put: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin.name} value=#{bin.value}")

  client.put(key, [bin], Shared.write_policy)

  bin = Bin.new(bin_name, "genvalue2")
  Shared.logger.info("Put: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin.name} value=#{bin.value}")

  client.put(key, [bin], Shared.write_policy)

  # Retrieve record and its generation count.
  record = client.get(key, [bin.name], Shared.policy)

  if record.nil?
    Shared.logger.fatal(
      "Failed to get: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key}")
    exit
  end

  received = record.bins[bin.name]
  expected = bin.value

  if received == expected
    Shared.logger.info("Get successful: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin.name} value=#{bin.value} generation=#{record.generation}")
  else
    Shared.logger.fatal("Get mismatch: Expected %s. Received %s.", expected, received)
  end

  # Set record and fail if it's not the expected generation.
  bin = Bin.new(bin_name, "genvalue3")
  Shared.logger.info("Put: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin.name} value=#{bin.value} expected generation=#{record.generation}")

  write_policy = WritePolicy.new
  write_policy.generation_policy = GenerationPolicy::EXPECT_GEN_EQUAL
  write_policy.generation = record.generation
  client.put(key, [bin], write_policy)

  # Set record with invalid generation and check results .
  bin = Bin.new(bin_name, "genvalue4")
  write_policy.generation = 9999
  Shared.logger.info("Put: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin.name} value=#{bin.value} expected generation=#{record.generation}")

  begin
    client.put(key, [bin], write_policy)
  rescue => ae
    if ae.is_a?(Exceptions::Aerospike)
      if ae.result_code == ResultCode::GENERATION_ERROR
        Shared.logger.info("Success: Generation error returned as expected.")
      else
        Shared.logger.fatal("Unexpected set return code: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin.name} value=#{bin.value} error=#{ae}")
        exit
      end
    else
      Shared.logger.fatal("Should have received generation error instead of success.")
      exit
    end
  end

  # Verify results.
  record = client.get(key, [bin.name], Shared.policy)

  if record.nil?
    Shared.logger.fatal("Failed to get: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key}")
    exit
  end

  received = record.bins[bin.name]
  expected = "genvalue3"

  if received == expected
    Shared.logger.info("Get successful: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin.name} value=#{received} generation=#{record.generation}")
  else
    Shared.logger.fatal("Get mismatch: Expected #{expected}. Received #{received}.")
    exit
  end
end

main