# Copyright 2012-2017 Aerospike, Inc.#
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
  expire_example(Shared.client)
  no_expire_example(Shared.client)

  Shared.logger.info("Example finished successfully.")
end

#
# Write and twice read an expiration record.
#
def expire_example(client)
  key = Key.new(Shared.namespace, Shared.set_name, "expirekey ")
  bin = Bin.new("expirebin", "expirevalue")

  Shared.logger.info("Put: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin.name} value=#{bin.value} ttl=2")

  # Specify that record expires 2 seconds after it's written.
  client.put(key, [bin], ttl: 2)

  # Read the record before it expires, showing it is there.
  Shared.logger.info("Get: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key}")

  record = client.get(key, [bin.name], Shared.policy)

  received = record.bins[bin.name]
  expected = bin.value

  if received == expected
    Shared.logger.info("Get record successful: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin.name} value=#{received}")
  else
    Shared.logger.fatal("Expire record mismatch: Expected #{expected}. Received #{received}.")
    exit
  end

  # Read the record after it expires, showing it's gone.
  Shared.logger.info("Sleeping for 3 seconds ...")
  sleep(3)

  record = client.get(key, [bin.name], Shared.policy)

  if record.nil?
    Shared.logger.info("Expiry of record successful. Record not found.")
  else
    Shared.logger.fatal("Found record when it should have expired.")
    exit
  end
end

#
# Write and twice read a non-expiring tuple using the new "NoExpire" value (-1).
# This example is most effective when the Default Namespace Time To Live (TTL)
# is set to a small value, such as 5 seconds.  When we sleep beyond that
# time, we show that the NoExpire TTL flag actually works.
#
def no_expire_example(client)
  key = Key.new(Shared.namespace, Shared.set_name, "expirekey")
  bin = Bin.new("expirebin", "noexpirevalue")

  Shared.logger.info("Put: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin.name} value=#{bin.value} ttl=NeverExpire")

  # Specify that record NEVER expires.
  write_policy = WritePolicy.new
  write_policy.generation = 2
  write_policy.ttl = Aerospike::TTL::NEVER_EXPIRE
  client.put(key, [bin], write_policy)

  # Read the record, showing it is there.
  Shared.logger.info("Get: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key}")

  record = client.get(key, [bin.name], Shared.policy)

  if record.nil?
    Shared.logger.fatal("Failed to get record: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key}")
    exit
  end

  received = record.bins[bin.name]
  expected = bin.value
  if received == expected
    Shared.logger.info("Get record successful: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin.name} value=#{received}")
  else
    Shared.logger.fatal("Expire record mismatch: Expected #{expected}. Received #{received}.")
    exit
  end

  # Read this Record after the Default Expiration, showing it is still there.
  # We should have set the Namespace TTL at 5 sec.
  Shared.logger.info("Sleeping for 10 seconds ...")
  sleep(10)

  record = client.get(key, [bin.name], Shared.policy)

  if record.nil?
    Shared.logger.fatal("Record expired and should NOT have.")
    exit
  else
    Shared.logger.info("Found Record (correctly) after default TTL.")
  end
end

main