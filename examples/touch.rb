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
	key = Key.new(Shared.namespace, Shared.set_name, "touchkey")
	bin = Bin.new("touchbin", "touchvalue")

	Shared.logger.info("Create record with 2 second expiration.")
	write_policy = WritePolicy.new
	write_policy.generation = 2
	client.put(key, [bin], write_policy)

	Shared.logger.info("Touch same record with 5 second expiration.")
	write_policy.expiration = 5
	record = client.operate(key, [Operation.touch, Operation.get_header], write_policy)

	if record.nil?
		Shared.logger.fatal("Failed to get: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin.name} value=nil")
		exit
	end

	if record.expiration == 0
		Shared.logger.fatal("Failed to get record expiration: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key}")
		exit
	end

	Shared.logger.info("Sleep 3 seconds.")
	sleep(3)

	record = client.get(key, [bin.name], Shared.policy)
	
	if record.nil?
		Shared.logger.fatal("Failed to get: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key}")
		exit
	end

	Shared.logger.info("Success. Record still exists.")
	Shared.logger.info("Sleep 4 seconds.")
	sleep(4)

	record = client.get(key, [bin.name], Shared.policy)

	if record.nil?
		Shared.logger.info("Success. Record expired as expected.")
	else
		Shared.logger.fatal("Found record when it should have expired.")
	end
end

main