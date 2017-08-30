# encoding: utf-8
# Copyright 2016 Aerospike, Inc.
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

describe "client.operate() - CDT List Operations", skip: !Support.feature?("cdt-list") do

  let(:client) { Support.client }
  let(:key) { Support.gen_random_key }
  let(:policy) { Aerospike::WritePolicy.new(record_exists_action: Aerospike::RecordExistsAction::REPLACE, ttl: 600) }

  def verifyOperation(record, operation, expectedResult, expectedRecordPostOp)
    client.put(key, record, policy)
    result = client.operate(key, [operation])
    expect(result.bins).to eql(expectedResult)
    record = client.get(key)
    expect(record.bins).to eql(expectedRecordPostOp)
  end

  describe "ListOperation.append" do
    it "appends a single item to the list and returns the list size" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.append('list', 99)
      expectedResult = { "list" => 6 }
      expectedRecord = { "list" => [1, 2, 3, 4, 5, 99] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end

    it "appends a single list item to the list and returns the list size" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.append('list', [99, 100])
      expectedResult = { "list" => 6 }
      expectedRecord = { "list" => [1, 2, 3, 4, 5, [99, 100]] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end

    it "appends multiple items to the list and returns the list size" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.append('list', 99, 100)
      expectedResult = { "list" => 7 }
      expectedRecord = { "list" => [1, 2, 3, 4, 5, 99, 100] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "ListOperation.insert" do
    it "inserts a single item at the specified index and returns the list size" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.insert('list', 2, 99)
      expectedResult = { "list" => 6 }
      expectedRecord = { "list" => [1, 2, 99, 3, 4, 5] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end

    it "inserts a single list item at the specified index and returns the list size" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.insert('list', 2, [99, 100])
      expectedResult = { "list" => 6 }
      expectedRecord = { "list" => [1, 2, [99, 100], 3, 4, 5] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end

    it "inserts multiple items at the specified index and returns the list size" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.insert('list', 2, 99, 100)
      expectedResult = { "list" => 7 }
      expectedRecord = { "list" => [1, 2, 99, 100, 3, 4, 5] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "ListOperation.pop" do
    it "removes the item at the specified index and returns it" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.pop('list', 2)
      expectedResult = { "list" => 3 }
      expectedRecord = { "list" => [1, 2, 4, 5] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "ListOperation.pop_range" do
    it "removes the items in the specified range and returns them" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.pop_range('list', 2, 2)
      expectedResult = { "list" => [3, 4] }
      expectedRecord = { "list" => [1, 2, 5] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end

    it "removes and returns all items starting at the specified index if count is not specified" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.pop_range('list', 2)
      expectedResult = { "list" => [3, 4, 5] }
      expectedRecord = { "list" => [1, 2] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "ListOperation.remove" do
    it "removes the item at the specified index and returns the number of items removed" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.remove('list', 2)
      expectedResult = { "list" => 1 }
      expectedRecord = { "list" => [1, 2, 4, 5] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "ListOperation.remove_range" do
    it "removes the items in the specified range and returns the number of items removed" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.remove_range('list', 2, 2)
      expectedResult = { "list" => 2 }
      expectedRecord = { "list" => [1, 2, 5] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end

    it "removes all items starting at the specified index and returns the number of items removed if count is not specified" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.remove_range('list', 2)
      expectedResult = { "list" => 3 }
      expectedRecord = { "list" => [1, 2] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "ListOperation.trim" do
    it "removes all elements not within the specified range and returns the number of elements removed" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.trim('list', 1, 3)
      expectedResult = { "list" => 2 }
      expectedRecord = { "list" => [2, 3, 4] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "ListOperation.increment", skip: !Support.min_version?("3.15") do
    it "increments the list index by the specified value" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.increment('list', 2, 3)
      expectedResult = { "list" => 6 }
      expectedRecord = { "list" => [1, 2, 6, 4, 5] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end

    it "increments the list index by 1" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.increment('list', 2)
      expectedResult = { "list" => 4 }
      expectedRecord = { "list" => [1, 2, 4, 4, 5] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "ListOperation.set" do
    it "sets the item at the specified index" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.set('list', 2, 99)
      expectedResult = nil
      expectedRecord = { "list" => [1, 2, 99, 4, 5] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "ListOperation.clear" do
    it "removes all elements from the list" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.clear('list')
      expectedResult = nil
      expectedRecord = { "list" => [] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "ListOperation.size" do
    it "returns the element count" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.size('list')
      expectedResult = { "list" => 5 }
      expectedRecord = { "list" => [1, 2, 3, 4, 5] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end

  describe "ListOperation.get" do
    it "returns the item at the specified index" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.get('list', 2)
      expectedResult = { "list" => 3 }
      expectedRecord = { "list" => [1, 2, 3, 4, 5] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end

    it "returns an error if the index is out of bounds" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.get('list', 99)
      expectedResult = { "list" => 3 }
      expectedRecord = { "list" => [1, 2, 3, 4, 5] }
      expect { verifyOperation(record, operation, expectedResult, expectedRecord) }.to raise_error(/Parameter error/)
    end
  end

  describe "ListOperation.get_range" do
    it "returns the items in the specified range" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.get_range('list', 1, 3)
      expectedResult = { "list" => [2, 3, 4]}
      expectedRecord = { "list" => [1, 2, 3, 4, 5] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end

    it "returns all items starting at the specified index if count is not specified" do
      record = { "list" => [1, 2, 3, 4, 5] }
      operation = Aerospike::CDT::ListOperation.get_range('list', 1)
      expectedResult = { "list" => [2, 3, 4, 5]}
      expectedRecord = { "list" => [1, 2, 3, 4, 5] }
      verifyOperation(record, operation, expectedResult, expectedRecord)
    end
  end
end
