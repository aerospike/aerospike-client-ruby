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

@udf = <<EOF
local function putBin(r,name,value)
    if not aerospike:exists(r) then aerospike:create(r) end
    r[name] = value
    aerospike:update(r)
end

-- Set a particular bin
function writeBin(r,name,value)
    putBin(r,name,value)
end

-- Get a particular bin
function readBin(r,name)
    return r[name]
end

-- Return generation count of record
function getGeneration(r)
    return record.gen(r)
end

-- Update record only if gen hasn't changed
function writeIfGenerationNotChanged(r,name,value,gen)
    if record.gen(r) == gen then
        r[name] = value
        aerospike:update(r)
    end
end

-- Set a particular bin only if record does not already exist.
function writeUnique(r,name,value)
    if not aerospike:exists(r) then 
        aerospike:create(r) 
        r[name] = value
        aerospike:update(r)
    end
end

-- Validate value before writing.
function writeWithValidation(r,name,value)
    if (value >= 1 and value <= 10) then
        putBin(r,name,value)
    else
        error("1000:Invalid value") 
    end
end

-- Record contains two integer bins, name1 and name2.
-- For name1 even integers, add value to existing name1 bin.
-- For name1 integers with a multiple of 5, delete name2 bin.
-- For name1 integers with a multiple of 9, delete record. 
function processRecord(r,name1,name2,addValue)
    local v = r[name1]

    if (v % 9 == 0) then
        aerospike:remove(r)
        return
    end

    if (v % 5 == 0) then
        r[name2] = nil
        aerospike:update(r)
        return
    end

    if (v % 2 == 0) then
        r[name1] = v + addValue
        aerospike:update(r)
    end
end

-- Set expiration of record
-- function expire(r,ttl)
--    if record.ttl(r) == gen then
--        r[name] = value
--        aerospike:update(r)
--    end
-- end
EOF

def main
    Shared.init
	register(Shared.client)
	writeUsingUdf(Shared.client)
	writeIfGenerationNotChanged(Shared.client)
	writeIfNotExists(Shared.client)
	writeWithValidation(Shared.client)
	writeListMapUsingUdf(Shared.client)
	writeBlobUsingUdf(Shared.client)

	Shared.logger.info("Example finished successfully.")
end

def register(client)
	task = client.register_udf(@udf, "record_example.lua", Language::LUA, Shared.write_policy)
	
	task.wait_till_completed
end

def writeUsingUdf(client)
	key = Key.new(Shared.namespace, Shared.set_name, "udfkey1")
	bin = Bin.new("udfbin1", "string value")

	client.execute_udf(key, "record_example", "writeBin",  [bin.name, bin.value], Shared.write_policy)

	record = client.get(key, [bin.name], Shared.policy)
	
	expected = bin.value
	received = record.bins[bin.name]

	if received == expected
		Shared.logger.info("Data matched: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin.name} value=#{received}")
	else
		Shared.logger.info("Data mismatch: Expected #{expected}. Received #{received}")
	end
end

def writeIfGenerationNotChanged(client)
	key = Key.new(Shared.namespace, Shared.set_name, "udfkey2")
	bin = Bin.new("udfbin2", "string value")

	# Seed record.
	client.put(key, [bin], Shared.write_policy)

	# Get record generation.
	gen = client.execute_udf(key, "record_example", "getGeneration", [], Shared.write_policy)
	
	# Write record if generation has not changed.
	client.execute_udf(key, "record_example", "writeIfGenerationNotChanged", [bin.name, bin.value, gen], Shared.write_policy)
	Shared.logger.info("Record written.")
end

def writeIfNotExists(client)
	key = Key.new(Shared.namespace, Shared.set_name, "udfkey3")
	bin_name = "udfbin3"

	# Delete record if it already exists.
	client.delete(key, Shared.write_policy)

	# Write record only if not already exists. This should succeed.
	client.execute_udf(key, "record_example", "writeUnique", [bin_name, "first"], Shared.write_policy)

	# Verify record written.
	record = client.get(key, [bin_name], Shared.policy)
	
	expected = "first"
	received = record.bins[bin_name]

	if received == expected
		Shared.logger.info("Record written: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin_name} value=#{received}")
	else
		Shared.logger.info("Data mismatch: Expected #{expected}. Received #{received}")
	end

	# Write record second time. This should fail.
	Shared.logger.info("Attempt second write.")
	client.execute_udf(key, "record_example", "writeUnique", [bin_name, "second"], Shared.write_policy)

	# Verify record not written.
	record = client.get(key, [bin_name], Shared.policy)
	
	received = record.bins[bin_name]

	if received == expected
		Shared.logger.info("Success. Record remained unchanged: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin_name} value=#{received}")
	else
		Shared.logger.info("Data mismatch: Expected #{expected}. Received #{received}")
	end
end

def writeWithValidation(client)
	key = Key.new(Shared.namespace, Shared.set_name, "udfkey4")
	bin_name = "udfbin4"

	# Lua function writeWithValidation accepts number between 1 and 10.
	# Write record with valid value.
	Shared.logger.info("Write with valid value.")
	client.execute_udf(key, "record_example", "writeWithValidation", [bin_name, 4], Shared.write_policy)

	# Write record with invalid value.
	Shared.logger.info("Write with invalid value.")

	begin
		client.execute_udf(key, "record_example", "writeWithValidation", [bin_name, 11], Shared.write_policy)
		Shared.logger.info("UDF should not have succeeded!")
		exit
	rescue
		Shared.logger.info("Success. UDF resulted in exception as expected.")
	end
end

def writeListMapUsingUdf(client)
	key = Key.new(Shared.namespace, Shared.set_name, "udfkey5")

	inner = ["string2", 8]
	innerMap = {"a" => 1, 2 => "b", "list" => inner}
	list = ["string1", 4, inner, innerMap]

	bin_name = "udfbin5"

	client.execute_udf(key, "record_example", "writeBin", [bin_name, list], Shared.write_policy)

	received = client.execute_udf(key, "record_example", "readBin", [bin_name], Shared.write_policy)
	

	if received == list
		Shared.logger.info("UDF data matched: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin_name} value=#{received}")
	else
		Shared.logger.info("UDF data mismatch")
		Shared.logger.info("Expected #{list}")
		Shared.logger.info("Received #{received}")
		exit
	end
end

def writeBlobUsingUdf(client)
	key = Key.new(Shared.namespace, Shared.set_name, "udfkey6")
	bin_name = "udfbin6"

	# Create packed blob using standard java tools.
	dos = "Hello world."
	blob = dos.bytes

	client.execute_udf(key, "record_example", "writeBin", [bin_name, blob], Shared.write_policy)
	received = client.execute_udf(key, "record_example", "readBin", [bin_name], Shared.write_policy)
	

	if blob == received
		Shared.logger.info("Blob data matched: namespace=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin_name} value=#{received}")
	else
		Shared.logger.fatal("Mismatch: expected=#{blob} received =#{received}")
		exit
	end
end

main