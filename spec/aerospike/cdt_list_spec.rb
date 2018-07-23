# encoding: utf-8
# Copyright 2016-2018 Aerospike, Inc.
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

describe "client.operate() - CDT List Operations", skip: !Support.feature?("cdt-list") do

  let(:client) { Support.client }
  let(:key) { Support.gen_random_key }
  let(:bins) { { "list" => [] } }
  let(:policy) {
    Aerospike::WritePolicy.new(
      record_exists_action: Aerospike::RecordExistsAction::REPLACE,
      ttl: 600
    )
  }

  before(:each) do
    client.put(key, bins, policy)
  end

  describe "ListOperation.append" do
    let(:bins) { { "list" => [1, 2, 3, 4, 5] } }

    it "appends a single item to the list and returns the list size" do
      operation = ListOperation.append("list", 99)
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to be(6)
      expect(client.get(key).bins["list"]).to eql([1, 2, 3, 4, 5, 99])
    end

    it "appends a single list item to the list and returns the list size" do
      operation = ListOperation.append("list", [99, 100])
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to be(6)
      expect(client.get(key).bins["list"]).to eql([1, 2, 3, 4, 5, [99, 100]])
    end

    it "appends multiple items to the list and returns the list size" do
      operation = ListOperation.append("list", 99, 100)
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to be(7)
      expect(client.get(key).bins["list"]).to eql([1, 2, 3, 4, 5, 99, 100])
    end
  end

  describe "ListOperation.insert" do
    let(:bins) { { "list" => [1, 2, 3, 4, 5] } }

    it "inserts a single item at the specified index and returns the list size" do
      operation = ListOperation.insert("list", 2, 99)
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to be(6)
      expect(client.get(key).bins["list"]).to eql([1, 2, 99, 3, 4, 5])
    end

    it "inserts a single list item at the specified index and returns the list size" do
      operation = ListOperation.insert("list", 2, [99, 100])
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to be(6)
      expect(client.get(key).bins["list"]).to eql([1, 2, [99, 100], 3, 4, 5])
    end

    it "inserts multiple items at the specified index and returns the list size" do
      operation = ListOperation.insert("list", 2, 99, 100)
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to be(7)
      expect(client.get(key).bins["list"]).to eql([1, 2, 99, 100, 3, 4, 5])
    end
  end

  describe "ListOperation.pop" do
    let(:bins) { { "list" => [1, 2, 3, 4, 5] } }

    it "removes the item at the specified index and returns it" do
      operation = ListOperation.pop("list", 2)
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to be(3)
      expect(client.get(key).bins["list"]).to eql([1, 2, 4, 5])
    end
  end

  describe "ListOperation.pop_range" do
    let(:bins) { { "list" => [1, 2, 3, 4, 5] } }

    it "removes the items in the specified range and returns them" do
      operation = ListOperation.pop_range("list", 2, 2)
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to eql([3, 4])
      expect(client.get(key).bins["list"]).to eql([1, 2, 5])
    end

    it "removes and returns all items starting at the specified index if count is not specified" do
      operation = ListOperation.pop_range("list", 2)
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to eql([3, 4, 5])
      expect(client.get(key).bins["list"]).to eql([1, 2])
    end
  end

  describe "ListOperation.remove" do
    let(:bins) { { "list" => [1, 2, 3, 4, 5] } }

    it "removes the item at the specified index and returns the number of items removed" do
      operation = ListOperation.remove("list", 2)
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to be(1)
      expect(client.get(key).bins["list"]).to eql([1, 2, 4, 5])
    end
  end

  describe "ListOperation.remove_range" do
    let(:bins) { { "list" => [1, 2, 3, 4, 5] } }

    it "removes the items in the specified range and returns the number of items removed" do
      operation = ListOperation.remove_range("list", 2, 2)
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to be(2)
      expect(client.get(key).bins["list"]).to eql([1, 2, 5])
    end

    it "removes all items starting at the specified index and returns the number of items removed if count is not specified" do
      operation = ListOperation.remove_range("list", 2)
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to be(3)
      expect(client.get(key).bins["list"]).to eql([1, 2])
    end
  end

  describe "ListOperation.trim" do
    let(:bins) { { "list" => [1, 2, 3, 4, 5] } }

    it "removes all elements not within the specified range and returns the number of elements removed" do
      operation = ListOperation.trim("list", 1, 3)
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to be(2)
      expect(client.get(key).bins["list"]).to eql([2, 3, 4])
    end
  end

  describe "ListOperation.increment", skip: !Support.min_version?("3.15") do
    let(:bins) { { "list" => [1, 2, 3, 4, 5] } }

    it "increments the list index by the specified value" do
      operation = ListOperation.increment("list", 2, 3)
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to be(6)
      expect(client.get(key).bins["list"]).to eql([1, 2, 6, 4, 5])
    end

    it "increments the list index by 1" do
      operation = ListOperation.increment("list", 2)
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to be(4)
      expect(client.get(key).bins["list"]).to eql([1, 2, 4, 4, 5])
    end
  end

  describe "ListOperation.set" do
    let(:bins) { { "list" => [1, 2, 3, 4, 5] } }

    it "sets the item at the specified index" do
      operation = ListOperation.set("list", 2, 99)
      result = client.operate(key, [operation])

      expect(result.bins).to be(nil)
      expect(client.get(key).bins["list"]).to eql([1, 2, 99, 4, 5])
    end
  end

  describe "ListOperation.clear" do
    let(:bins) { { "list" => [1, 2, 3, 4, 5] } }

    it "removes all elements from the list" do
      operation = ListOperation.clear("list")
      result = client.operate(key, [operation])

      expect(result.bins).to be(nil)
      expect(client.get(key).bins["list"]).to eql([])
    end
  end

  describe "ListOperation.size" do
    let(:bins) { { "list" => [1, 2, 3, 4, 5] } }

    it "returns the element count" do
      operation = ListOperation.size("list")
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to be(5)
    end
  end

  describe "ListOperation.get" do
    let(:bins) { { "list" => [1, 2, 3, 4, 5] } }

    it "returns the item at the specified index" do
      operation = ListOperation.get("list", 2)
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to be(3)
    end

    it "returns an error if the index is out of bounds" do
      operation = ListOperation.get("list", 99)

      expect { client.operate(key, [operation]) }.to raise_error(/Parameter error/)
    end
  end

  describe "ListOperation.get_range" do
    let(:bins) { { "list" => [1, 2, 3, 4, 5] } }

    it "returns the items in the specified range" do
      operation = ListOperation.get_range("list", 1, 3)
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to eql([2, 3, 4])
    end

    it "returns all items starting at the specified index if count is not specified" do
      operation = ListOperation.get_range("list", 1)
      result = client.operate(key, [operation])

      expect(result.bins["list"]).to eql([2, 3, 4, 5])
    end
  end
end
