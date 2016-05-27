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
  test_list_strings(Shared.client)
  test_list_complex(Shared.client)
  test_map_strings(Shared.client)
  test_map_complex(Shared.client)
  test_list_map_combined(Shared.client)

  Shared.logger.info("Example finished successfully.")
end

#*
# Write/Read []string directly instead of relying on java serializer.
#
def test_list_strings(client)
  Shared.logger.info("Read/Write string array")
  key = Key.new(Shared.namespace, Shared.set_name, "listkey1")
  client.delete(key, Shared.write_policy)

  list = ["string1", "string2", "string3"]

  bin = Bin.new("listbin1", list)
  client.put(key, [bin], Shared.write_policy)

  record = client.get(key, [bin.name], Shared.policy)

  received_list = record.bins[bin.name]

  validate_size(3, received_list.length)
  validate("string1", received_list[0])
  validate("string2", received_list[1])
  validate("string3", received_list[2])

  Shared.logger.info("Read/Write string array successful.")
end

#*
# Write/Read []interface{end directly instead of relying on java serializer.
#
def test_list_complex(client)
  Shared.logger.info("Read/Write Any => Any")
  key = Key.new(Shared.namespace, Shared.set_name, "listkey2")
  client.delete(key, Shared.write_policy)

  blob = [3, 52, 125]
  list = ["string1", 2, blob]

  bin = Bin.new("listbin2", list)
  client.put(key, [bin], Shared.write_policy)

  record = client.get(key, [bin.name], Shared.policy)

  received_list = record.bins[bin.name]

  validate_size(3, received_list.length)
  validate("string1", received_list[0])
  # Server convert numbers to long, so must expect long.
  validate(2, received_list[1])
  validate(blob, received_list[2])

  Shared.logger.info("Read/Write map Any => Any successful.")
end

#*
# Write/Read map[string]string directly instead of relying on java serializer.
#
def test_map_strings(client)
  Shared.logger.info("Read/Write map string => string")
  key = Key.new(Shared.namespace, Shared.set_name, "mapkey1")
  client.delete(key, Shared.write_policy)

  amap = {
    "key1" => "string1",
    "key2" => "string2",
    "key3" => "string3",
  }

  bin = Bin.new("mapbin1", amap)
  client.put(key, [bin], Shared.write_policy)

  record = client.get(key, [bin.name], Shared.policy)

  received_map = record.bins[bin.name]

  validate_size(3, received_map.length)
  validate("string1", received_map["key1"])
  validate("string2", received_map["key2"])
  validate("string3", received_map["key3"])

  Shared.logger.info("Read/Write map string => string successful")
end

#*
# Write/Read map[interface{end]interface{end directly instead of relying on java serializer.
#
def test_map_complex(client)
  Shared.logger.info("Read/Write map Any => Any")
  key = Key.new(Shared.namespace, Shared.set_name, "mapkey2")
  client.delete(key, Shared.write_policy)

  blob = [3, 52, 125]
  list = [
    100034,
    12384955,
    3,
    512,
  ]

  amap = {
    "key1" => "string1",
    "key2" => 2,
    "key3" => blob,
    "key4" => list,
  }

  bin = Bin.new("mapbin2", amap)
  client.put(key, [bin], Shared.write_policy)

  record = client.get(key, [bin.name], Shared.policy)

  received_map = record.bins[bin.name]

  validate_size(4, received_map.length)
  validate("string1", received_map["key1"])
  # Server convert numbers to long, so must expect long.
  validate(2, received_map["key2"])
  validate(blob, received_map["key3"])

  received_inner = received_map["key4"]
  validate_size(4, received_inner.length)
  validate(100034, received_inner[0])
  validate(12384955, received_inner[1])
  validate(3, received_inner[2])
  validate(512, received_inner[3])

  Shared.logger.info("Read/Writemap Any => Any successful")
end

#*
# Write/Read Array/Map combination directly instead of relying on java serializer.
#
def test_list_map_combined(client)
  Shared.logger.info("Read/Write Array/Map")
  key = Key.new(Shared.namespace, Shared.set_name, "listmapkey")
  client.delete(key, Shared.write_policy)

  blob = [3, 52, 125]
  inner = [
    "string2",
    5,
  ]

  inner_map = {
    "a" =>    1,
    2 =>      "b",
    3 =>      blob,
    "list" => inner,
  }

  list = [
    "string1",
    8,
    inner,
    inner_map,
  ]

  bin = Bin.new("listmapbin", list)
  client.put(key, [bin], Shared.write_policy)

  record = client.get(key, [bin.name], Shared.policy)

  received = record.bins[bin.name]

  validate_size(4, received.length)
  validate("string1", received[0])
  # Server convert numbers to long, so must expect long.
  validate(8, received[1])

  received_inner = received[2]
  validate_size(2, received_inner.length)
  validate("string2", received_inner[0])
  validate(5, received_inner[1])

  received_map = received[3]
  validate_size(4, received_map.length)
  validate(1, received_map["a"])
  validate("b", received_map[2])
  validate(blob, received_map[3])

  received_inner2 = received_map["list"]
  validate_size(2, received_inner2.length)
  validate("string2", received_inner2[0])
  validate(5, received_inner2[1])

  Shared.logger.info("Read/Write Array/Map successful")
end

def validate_size(expected, received)
  if received != expected
    Shared.logger.fatal("Size mismatch: expected=#{expected} received =#{received}")
    exit
  end
end

def validate(expected, received)
  if !(received == expected)
    Shared.logger.fatal("Mismatch: expected=#{expected} received =#{received}")
    exit
  end
end

main