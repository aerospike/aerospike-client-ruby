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
  run_replace_example(Shared.client)
  run_replace_only_example(Shared.client)

  Shared.logger.info("Example finished successfully.")
end

def run_replace_example(client)
  key = Key.new(Shared.namespace, Shared.set_name, "replacekey")

  bin1 = Bin.new("bin1", "value1")
  bin2 = Bin.new("bin2", "value2")
  bin3 = Bin.new("bin3", "value3")

  Shared.logger.info("Put: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin1=#{bin1.name} value1=#{bin1.value} bin2=#{bin2.name} value2=#{bin2.value}")

  client.put(key, [bin1, bin2], Shared.write_policy)

  Shared.logger.info("Replace with: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin3.name} value=#{bin3.value}")

  wpolicy = WritePolicy.new
  wpolicy.record_exists_action = RecordExistsAction::REPLACE
  client.put(key, [bin3], wpolicy)

  Shared.logger.info("Get: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key}")

  record = client.get(key, [], Shared.policy)

  if record.nil?
    Shared.logger.fatal("Failed to get: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key}")
    exit
  end

  if record.bins[bin1.name].nil?
    Shared.logger.info(bin1.name + " was deleted as expected.")
  else
    Shared.logger.fatal(bin1.name + " found when it should have been deleted.")
    exit
  end

  if record.bins[bin2.name].nil?
    Shared.logger.info(bin2.name + " was deleted as expected.")
  else
    Shared.logger.fatal(bin2.name + " found when it should have been deleted.")
  end

  Shared.validate_bin(key, bin3, record)
end

def run_replace_only_example(client)
  key = Key.new(Shared.namespace, Shared.set_name, "replaceonlykey")

  bin = Bin.new("bin", "value")

  # Delete record if it already exists.
  client.delete(key, Shared.write_policy)

  Shared.logger.info("Replace record requiring that it exists: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key}")

  wpolicy = WritePolicy.new
  wpolicy.record_exists_action = RecordExistsAction::REPLACE_ONLY

  begin
    client.put(key, [bin], wpolicy)
  rescue => ae
    if ae.is_a?(Exceptions::Aerospike) && ae.result_code == ResultCode::KEY_NOT_FOUND_ERROR
      Shared.logger.info("Success. `#{ae}` exception returned as expected.")
    else
      Shared.logger.fatal("Failure. This command should have resulted in an error.")
      exit
    end
  end
end

main