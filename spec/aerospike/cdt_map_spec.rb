# encoding: utf-8
# Copyright 2014 Aerospike, Inc.
#
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

require "spec_helper"
include Aerospike::CDT

describe "client.operate() - CDT Map Operations", skip: !Support.feature?("cdt-map") do

  let(:client) { Support.client }
  let(:key) { Support.gen_random_key }

  def verifyOperation(record, operations, expectedResult, expectedRecordPostOp = record, policy: MapPolicy::DEFAULT)
    if record
      put_items = MapOperation.put_items("map", record["map"], policy: policy)
      client.operate(key, [put_items])
    end
    result = client.operate(key, Array(operations))
    expect(result.bins).to eql(expectedResult)
    record = client.get(key)
    expect(record.bins).to eql(expectedRecordPostOp)
  end

  describe "MapOperation.set_policy" do
    it "changes the map order" do
      record = { "map" => { "c" => 1, "b" => 2, "a" => 3 } }
      operations = [
        MapOperation.set_policy("map", MapPolicy.new(order: MapOrder::KEY_ORDERED)),
        MapOperation.get_key_range("map", "a", "z", return_type: MapReturnType::KEY)
      ]
      expectedResult = { "map" => [ "a", "b", "c" ] }
      verifyOperation(record, operations, expectedResult, policy: MapPolicy.new(order: MapOrder::UNORDERED))
    end
  end

  describe "MapOperation.put" do
    it "adds the item to the map and returns the map size" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.put("map", "x", 99)
      expectedResult = { "map" => 4 }
      expectedRecord = { "map" => { "a" => 1, "b" => 2, "c" => 3, "x" => 99 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end

    it "creates a new map if it doesn't exist yet" do
      operation = MapOperation.put("map", "a", 1)
      expectedResult = { "map" => 1 }
      expectedRecord = { "map" => { "a" => 1 } }
      verifyOperation(nil, operation, expectedResult, expectedRecord)
    end

    context "MapWriteMode::UPDATE_ONLY" do
      it "overwrites an existing key" do
        record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
        operation = MapOperation.put("map", "b", 99)
        expectedResult = { "map" => 3 }
        expectedRecord = { "map" => { "a" => 1, "b" => 99, "c" => 3 } }
        verifyOperation(record, operation, expectedResult, expectedRecord)
      end

      it "fails to write a non-existing key" do
        record = { "map" => { "a" => 1, "c" => 3 } }
        policy = MapPolicy.new(write_mode: MapWriteMode::UPDATE_ONLY)
        operation = MapOperation.put("map", "b", 99, policy: policy)
        expectedResult = { "map" => 2 }
        expectedRecord = { "map" => { "a" => 1, "c" => 3 } }
        verifyOperation(record, operation, expectedResult, expectedRecord)
      end
    end

    context "MapWriteMode::CREATE_ONLY" do
      it "fails to write an existing key" do
        record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
        policy = MapPolicy.new(write_mode: MapWriteMode::CREATE_ONLY)
        operation = MapOperation.put("map", "b", 99, policy: policy)
        expectedResult = { "map" => 3 }
        expectedRecord = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
        verifyOperation(record, operation, expectedResult, expectedRecord)
      end

      it "creates a new key if it does not exist" do
        record = { "map" => { "a" => 1, "c" => 3 } }
        policy = MapPolicy.new(write_mode: MapWriteMode::CREATE_ONLY)
        operation = MapOperation.put("map", "b", 99, policy: policy)
        expectedResult = { "map" => 3 }
        expectedRecord = { "map" => { "a" => 1, "b" => 99, "c" => 3 } }
        verifyOperation(record, operation, expectedResult, expectedRecord)
      end
    end
  end

  describe "MapOperation.put_items" do
    it "adds the items to the map and returns the map size" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.put_items("map", { "x" => 99 })
      expectedResult = { "map" => 4 }
      expectedRecord = { "map" => { "a" => 1, "b" => 2, "c" => 3, "x" => 99 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "MapOperation.increment" do
    it "increments the value for all items identified by the key and returns the final result" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.increment("map", "b", 3)
      expectedResult = { "map" => 5 }
      expectedRecord = { "map" => { "a" => 1, "b" => 5, "c" => 3 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "MapOperation.decrement" do
    it "decrements the value for all items identified by the key and returns the final result" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.decrement("map", "b", 3)
      expectedResult = { "map" => -1 }
      expectedRecord = { "map" => { "a" => 1, "b" => -1, "c" => 3 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "MapOperation.clear" do
    it "removes all items from the map" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.clear("map")
      expectedResult = nil
      expectedRecord = { "map" => { } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "MapOperation.remove_keys" do
    it "removes a single key from the map" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.remove_keys("map", "b")
      expectedResult = nil
      expectedRecord = { "map" => { "a" => 1, "c" => 3 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end

    it "removes a list of keys from the map" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.remove_keys("map", "a", "b")
      expectedResult = nil
      expectedRecord = { "map" => { "c" => 3 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "MapOperation.remove_key_range" do
    it "removes the specified key range from the map" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.remove_key_range("map", "b", "c")
      expectedResult = nil
      expectedRecord = { "map" => { "a" => 1, "c" => 3 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end

    it "removes the all keys from the specified start key until the end" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.remove_key_range("map", "b")
      expectedResult = nil
      expectedRecord = { "map" => { "a" => 1 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end

    it "removes the all keys until the specified end key" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.remove_key_range("map", nil, "b")
      expectedResult = nil
      expectedRecord = { "map" => { "b" => 2, "c" => 3 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "MapOperation.remove_values" do
    it "removes the items identified by a single value" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3, "d" => 2 } }
      operation = MapOperation.remove_values("map", 2)
      expectedResult = nil
      expectedRecord = { "map" => { "a" => 1, "c" => 3 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end

    it "removes the items identified by a list of values" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3, "d" => 2 } }
      operation = MapOperation.remove_values("map", 2, 3)
      expectedResult = nil
      expectedRecord = { "map" => { "a" => 1 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "MapOperation.remove_value_range" do
    it "removes the specified value range from the map" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.remove_value_range("map", 2, 3)
      expectedResult = nil
      expectedRecord = { "map" => { "a" => 1, "c" => 3 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end

    it "removes all elements starting from the specified start value until the end" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.remove_value_range("map", 2)
      expectedResult = nil
      expectedRecord = { "map" => { "a" => 1 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end

    it "removes all elements until the specified end value" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.remove_value_range("map", nil, 3)
      expectedResult = nil
      expectedRecord = { "map" => { "c" => 3 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "MapOperation.remove_index" do
    it "removes a map item identified by index from the map" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.remove_index("map", 1)
      expectedResult = nil
      expectedRecord = { "map" => { "a" => 1, "c" => 3 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "MapOperation.remove_index_range" do
    it "removes 'count' map items starting at the specified index from the map" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.remove_index_range("map", 1, 2)
      expectedResult = nil
      expectedRecord = { "map" => { "a" => 1 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end

    it "removes all items starting at the specified index to the end of the map" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.remove_index_range("map", 1)
      expectedResult = nil
      expectedRecord = { "map" => { "a" => 1 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "MapOperation.remove_by_rank" do
    it "removes a map item identified by rank from the map" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.remove_by_rank("map", 1)
      expectedResult = nil
      expectedRecord = { "map" => { "a" => 1, "c" => 3 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "MapOperation.remove_by_rank_range" do
    it "removes 'count' map items starting at the specified rank from the map" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.remove_by_rank_range("map", 1, 2)
      expectedResult = nil
      expectedRecord = { "map" => { "a" => 1 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end

    it "removes all items starting at the specified rank to the end of the map" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.remove_by_rank_range("map", 1)
      expectedResult = nil
      expectedRecord = { "map" => { "a" => 1 } }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "MapOperation.size" do
    it "returns the size of the map" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.size("map")
      expectedResult = { "map" => 3 }
      verifyOperation(record, operation, expectedResult)
    end
  end

  describe "MapOperation.get_key" do
    it "gets a single key from the map" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.get_key("map", "b", return_type: MapReturnType::KEY_VALUE)
      expectedResult = { "map" => { "b" => 2 } }
      verifyOperation(record, operation, expectedResult)
    end
  end

  describe "MapOperation.get_key_range" do
    it "gets the specified key range from the map" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.get_key_range("map", "b", "c", return_type: MapReturnType::KEY_VALUE)
      expectedResult = { "map" => { "b" => 2 } }
      verifyOperation(record, operation, expectedResult)
    end

    it "gets all keys from the specified start key until the end" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.get_key_range("map", "b", return_type: MapReturnType::KEY_VALUE)
      expectedResult = { "map" => { "b" => 2, "c" => 3 } }
      verifyOperation(record, operation, expectedResult)
    end

    it "gets all keys from the start to the specified end key" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.get_key_range("map", nil, "b", return_type: MapReturnType::KEY_VALUE)
      expectedResult = { "map" => { "a" => 1 } }
      verifyOperation(record, operation, expectedResult)
    end
  end

  describe "MapOperation.get_value" do
    it "gets the item identified by a single value" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3, "d" => 2 } }
      operation = MapOperation.get_value("map", 2, return_type: MapReturnType::KEY_VALUE)
      expectedResult = { "map" => { "b" => 2, "d" => 2 } }
      verifyOperation(record, operation, expectedResult)
    end
  end

  describe "MapOperation.get_value_range" do
    it "gets the specified key range from the map" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3, "d" => 2} }
      operation = MapOperation.get_value_range("map", 2, 3, return_type: MapReturnType::KEY_VALUE)
      expectedResult = { "map" => { "b" => 2, "d" => 2 } }
      verifyOperation(record, operation, expectedResult)
    end

    it "gets all values from the specified start value until the end" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3, "d" => 2} }
      operation = MapOperation.get_value_range("map", 2, return_type: MapReturnType::KEY_VALUE)
      expectedResult = { "map" => { "b" => 2, "d" => 2, "c" => 3 } }
      verifyOperation(record, operation, expectedResult)
    end

    it "gets all values from the start of the map until the specified end value" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3, "d" => 2} }
      operation = MapOperation.get_value_range("map", nil, 3, return_type: MapReturnType::KEY_VALUE)
      expectedResult = { "map" => { "a" => 1, "b" => 2, "d" => 2 } }
      verifyOperation(record, operation, expectedResult)
    end
  end

  describe "MapOperation.get_index" do
    it "gets a map item identified by index from the map" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.get_index("map", 1, return_type: MapReturnType::KEY_VALUE)
      expectedResult = { "map" => { "b" => 2 } }
      verifyOperation(record, operation, expectedResult)
    end
  end

  describe "MapOperation.get_index_range" do
    it "gets 'count' map items starting at the specified index from the map" do
      record = { "map" => { "c" => 1, "b" => 2, "a" => 3 } }
      operation = MapOperation.get_index_range("map", 1, 2, return_type: MapReturnType::KEY)
      expectedResult = { "map" => [ "b", "c" ] }
      verifyOperation(record, operation, expectedResult)
    end

    it "gets all items starting at the specified index to the end of the map" do
      record = { "map" => { "c" => 1, "b" => 2, "a" => 3 } }
      operation = MapOperation.get_index_range("map", 1, return_type: MapReturnType::KEY)
      expectedResult = { "map" => [ "b", "c" ] }
      verifyOperation(record, operation, expectedResult)
    end
  end

  describe "MapOperation.get_by_rank" do
    it "gets a map item identified by rank from the map" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.get_by_rank("map", 1).and_return(MapReturnType::KEY_VALUE)
      expectedResult = { "map" => { "b" => 2 } }
      verifyOperation(record, operation, expectedResult)
    end
  end

  describe "MapOperation.get_by_rank_range" do
    it "gets 'count' map items starting at the specified rank from the map" do
      record = { "map" => { "a" => 3, "b" => 2, "c" => 1 } }
      operation = MapOperation.get_by_rank_range("map", 1, 2).and_return(MapReturnType::KEY)
      expectedResult = { "map" => [ "b", "a" ] }
      verifyOperation(record, operation, expectedResult)
    end

    it "gets all items starting at the specified rank to the end of the map" do
      record = { "map" => { "a" => 3, "b" => 2, "c" => 1 } }
      operation = MapOperation.get_by_rank_range("map", 1).and_return(MapReturnType::KEY)
      expectedResult = { "map" => [ "b", "a" ] }
      verifyOperation(record, operation, expectedResult)
    end
  end

  describe "MapOperation#and_return" do

    let(:map_policy) { MapPolicy.new(order: MapOrder::KEY_ORDERED) }

    it "returns nothing" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.get_key("map", "b").and_return(MapReturnType::NONE)
      expectedResult = { "map" => nil }
      verifyOperation(record, operation, expectedResult, policy: map_policy)
    end

    it "returns key index" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.get_key("map", "a").and_return(MapReturnType::INDEX)
      expectedResult = { "map" => 0 }
      verifyOperation(record, operation, expectedResult, policy: map_policy)
    end

    it "returns reverse key index" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.get_key("map", "a").and_return(MapReturnType::REVERSE_INDEX)
      expectedResult = { "map" => 2 }
      verifyOperation(record, operation, expectedResult, policy: map_policy)
    end

    it "returns value order (rank)" do
      record = { "map" => { "a" => 3, "b" => 2, "c" => 1 } }
      operation = MapOperation.get_key("map", "a").and_return(MapReturnType::RANK)
      expectedResult = { "map" => 2 }
      verifyOperation(record, operation, expectedResult, policy: map_policy)
    end

    it "returns reverse value order (reverse rank)" do
      record = { "map" => { "a" => 3, "b" => 2, "c" => 1 } }
      operation = MapOperation.get_key("map", "a").and_return(MapReturnType::REVERSE_RANK)
      expectedResult = { "map" => 0 }
      verifyOperation(record, operation, expectedResult, policy: map_policy)
    end

    it "returns count of items selected" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.get_key_range("map", "a", "c").and_return(MapReturnType::COUNT)
      expectedResult = { "map" => 2 }
      verifyOperation(record, operation, expectedResult, policy: map_policy)
    end

    it "returns key for a single read" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.get_index("map", 0).and_return(MapReturnType::KEY)
      expectedResult = { "map" => "a" }
      verifyOperation(record, operation, expectedResult, policy: map_policy)
    end

    it "returns keys for range read" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.get_index_range("map", 0, 2).and_return(MapReturnType::KEY)
      expectedResult = { "map" => [ "a", "b" ] }
      verifyOperation(record, operation, expectedResult, policy: map_policy)
    end

    it "returns value for a single read" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.get_index("map", 0).and_return(MapReturnType::VALUE)
      expectedResult = { "map" => 1 }
      verifyOperation(record, operation, expectedResult, policy: map_policy)
    end

    it "returns value for range read" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.get_index_range("map", 0, 2).and_return(MapReturnType::VALUE)
      expectedResult = { "map" => [ 1, 2] }
      verifyOperation(record, operation, expectedResult, policy: map_policy)
    end

    it "returns key/value for a single read" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.get_index("map", 0).and_return(MapReturnType::KEY_VALUE)
      expectedResult = { "map" => { "a" => 1 } }
      verifyOperation(record, operation, expectedResult, policy: map_policy)
    end

    it "returns key/value for a range read" do
      record = { "map" => { "a" => 1, "b" => 2, "c" => 3 } }
      operation = MapOperation.get_index_range("map", 0, 2).and_return(MapReturnType::KEY_VALUE)
      expectedResult = { "map" => { "a" => 1, "b" => 2 } }
      verifyOperation(record, operation, expectedResult, policy: map_policy)
    end
  end
end
