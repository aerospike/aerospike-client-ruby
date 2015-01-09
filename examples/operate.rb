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
	# Write initial record.
	key = Key.new(Shared.namespace, Shared.set_name, "opkey")
	bin1 = Bin.new("optintbin", 7)
	bin2 = Bin.new("optstringbin", "string value")
	Shared.logger.info("Put: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin1=#{bin1.name} value1=#{bin1.value} bin2=#{bin2.name} value2=#{bin2.value}")
	client.put(key, [bin1, bin2], Shared.write_policy)

	# Add integer, write new string and read record.
	bin3 = Bin.new(bin1.name, 4)
	bin4 = Bin.new(bin2.name, "new string")
	Shared.logger.info("Add: #{bin3.value}")
	Shared.logger.info("Write: #{bin4.value}")
	Shared.logger.info("Read:")

	record = client.operate(key, [Operation.add(bin3), Operation.put(bin4), Operation.get], Shared.write_policy)

	if record.nil?
		Shared.logger.fatal("Failed to get: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key}")
		exit
	end

	bin_expected = Bin.new(bin3.name, 11)
	Shared.validate_bin(key, bin_expected, record)
	Shared.validate_bin(key, bin4, record)
end

main