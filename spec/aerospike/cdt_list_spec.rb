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
  let(:list_bin) { "list" }
  let(:list_value) { [] }
  let(:return_type) { ListReturnType::VALUE }
  let(:order) { ListOrder::UNORDERED }
  let(:write_flags) { ListWriteFlags::DEFAULT }
  let(:list_policy) { ListPolicy.new(order: order, write_flags: write_flags) }

  before(:each) do
    # Use append op to create list so we can set list order
    create_policy = ListPolicy.new(order: list_policy.order)
    op = ListOperation.append(list_bin, *list_value, policy: create_policy)
    client.operate(key, [op])
  end

  def list_post_op
    client.get(key).bins[list_bin]
  end

  describe "ListOperation.append" do
    let(:list_value) { [1, 2, 3, 4, 5] }

    it "appends a single item to the list and returns the list size" do
      operation = ListOperation.append(list_bin, 99)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 6
      expect(list_post_op).to eql([1, 2, 3, 4, 5, 99])
    end

    it "appends a single list item to the list and returns the list size" do
      operation = ListOperation.append(list_bin, [99, 100])
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 6
      expect(list_post_op).to eql([1, 2, 3, 4, 5, [99, 100]])
    end

    it "appends multiple items to the list and returns the list size" do
      operation = ListOperation.append(list_bin, 99, 100)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 7
      expect(list_post_op).to eql([1, 2, 3, 4, 5, 99, 100])
    end
  end

  describe "ListOperation.insert" do
    let(:list_value) { [1, 2, 3, 4, 5] }

    it "inserts a single item at the specified index and returns the list size" do
      operation = ListOperation.insert(list_bin, 2, 99)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 6
      expect(list_post_op).to eql([1, 2, 99, 3, 4, 5])
    end

    it "inserts a single list item at the specified index and returns the list size" do
      operation = ListOperation.insert(list_bin, 2, [99, 100])
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 6
      expect(list_post_op).to eql([1, 2, [99, 100], 3, 4, 5])
    end

    it "inserts multiple items at the specified index and returns the list size" do
      operation = ListOperation.insert(list_bin, 2, 99, 100)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 7
      expect(list_post_op).to eql([1, 2, 99, 100, 3, 4, 5])
    end
  end

  describe "ListOperation.pop" do
    let(:list_value) { [1, 2, 3, 4, 5] }

    it "removes the item at the specified index and returns it" do
      operation = ListOperation.pop(list_bin, 2)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 3
      expect(list_post_op).to eql([1, 2, 4, 5])
    end
  end

  describe "ListOperation.pop_range" do
    let(:list_value) { [1, 2, 3, 4, 5] }

    it "removes the items in the specified range and returns them" do
      operation = ListOperation.pop_range(list_bin, 2, 2)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to eql([3, 4])
      expect(list_post_op).to eql([1, 2, 5])
    end

    it "removes and returns all items starting at the specified index if count is not specified" do
      operation = ListOperation.pop_range(list_bin, 2)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to eql([3, 4, 5])
      expect(list_post_op).to eql([1, 2])
    end
  end

  describe "ListOperation.remove" do
    let(:list_value) { [1, 2, 3, 4, 5] }

    it "removes the item at the specified index and returns the number of items removed" do
      operation = ListOperation.remove(list_bin, 2)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 1
      expect(list_post_op).to eql([1, 2, 4, 5])
    end
  end

  describe "ListOperation.remove_range" do
    let(:list_value) { [1, 2, 3, 4, 5] }

    it "removes the items in the specified range and returns the number of items removed" do
      operation = ListOperation.remove_range(list_bin, 2, 2)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 2
      expect(list_post_op).to eql([1, 2, 5])
    end

    it "removes all items starting at the specified index and returns the number of items removed if count is not specified" do
      operation = ListOperation.remove_range(list_bin, 2)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 3
      expect(list_post_op).to eql([1, 2])
    end
  end

  describe "ListOperation.trim" do
    let(:list_value) { [1, 2, 3, 4, 5] }

    it "removes all elements not within the specified range and returns the number of elements removed" do
      operation = ListOperation.trim(list_bin, 1, 3)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 2
      expect(list_post_op).to eql([2, 3, 4])
    end
  end

  describe "ListOperation.increment", skip: !Support.min_version?("3.15") do
    let(:list_value) { [1, 2, 3, 4, 5] }

    it "increments the list index by the specified value" do
      operation = ListOperation.increment(list_bin, 2, 3)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 6
      expect(list_post_op).to eql([1, 2, 6, 4, 5])
    end

    it "increments the list index by 1" do
      operation = ListOperation.increment(list_bin, 2)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 4
      expect(list_post_op).to eql([1, 2, 4, 4, 5])
    end
  end

  describe "ListOperation.set" do
    let(:list_value) { [1, 2, 3, 4, 5] }

    it "sets the item at the specified index" do
      operation = ListOperation.set(list_bin, 2, 99)
      result = client.operate(key, [operation])

      expect(result.bins).to be nil
      expect(list_post_op).to eql([1, 2, 99, 4, 5])
    end
  end

  describe "ListOperation.clear" do
    let(:list_value) { [1, 2, 3, 4, 5] }

    it "removes all elements from the list" do
      operation = ListOperation.clear(list_bin)
      result = client.operate(key, [operation])

      expect(result.bins).to be nil
      expect(list_post_op).to eql([])
    end
  end

  describe "ListOperation.size" do
    let(:list_value) { [1, 2, 3, 4, 5] }

    it "returns the element count" do
      operation = ListOperation.size(list_bin)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 5
    end
  end

  describe "ListOperation.get" do
    let(:list_value) { [1, 2, 3, 4, 5] }

    it "returns the item at the specified index" do
      operation = ListOperation.get(list_bin, 2)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 3
    end

    it "returns an error if the index is out of bounds" do
      operation = ListOperation.get(list_bin, 99)

      expect { client.operate(key, [operation]) }.to raise_error(/Parameter error/)
    end
  end

  describe "ListOperation.get_range" do
    let(:list_value) { [1, 2, 3, 4, 5] }

    it "returns the items in the specified range" do
      operation = ListOperation.get_range(list_bin, 1, 3)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to eql([2, 3, 4])
    end

    it "returns all items starting at the specified index if count is not specified" do
      operation = ListOperation.get_range(list_bin, 1)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to eql([2, 3, 4, 5])
    end
  end

  describe "ListOperation.get_by_index" do
    let(:list_value) { [1, 2, 3, 4, 5] }

    it "does not return anything if return type is not specified" do
      operation = ListOperation.get_by_index(list_bin, 2)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be nil
    end

    it "returns the value at the specified index" do
      operation = ListOperation.get_by_index(list_bin, 2)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 3
    end

    it "returns an error if the index is out of bounds" do
      operation = ListOperation.get_by_index(list_bin, 99)

      expect { client.operate(key, [operation]) }.to raise_error(/Parameter error/)
    end
  end

  describe "ListOperation.get_by_index_range" do
    let(:list_value) { [1, 2, 3, 4, 5] }

    it "returns the value at the specified index range" do
      operation = ListOperation.get_by_index_range(list_bin, 1, 3)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to eql([2, 3, 4])
    end

    it "returns all values starting at the specified index if count is not specified" do
      operation = ListOperation.get_by_index_range(list_bin, 1)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to eql([2, 3, 4, 5])
    end
  end

  describe "ListOperation.get_by_rank" do
    let(:list_value) { [3, 4, 1, 5, 2] }

    it "returns the value at the specified rank" do
      operation = ListOperation.get_by_rank(list_bin, 0)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 1
    end
  end

  describe "ListOperation.get_by_rank_range" do
    let(:list_value) { [3, 4, 1, 5, 2] }

    it "returns the value at the specified rank range" do
      operation = ListOperation.get_by_rank_range(list_bin, 1, 3)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to contain_exactly(2, 3, 4)
    end

    it "returns all values starting at the specified index if count is not specified" do
      operation = ListOperation.get_by_rank_range(list_bin, 3)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to contain_exactly(4, 5)
    end
  end

  describe "ListOperation.get_by_value" do
    let(:list_value) { [1, 4, 2, 3, 5, 1, 2] }
    let(:return_type) { ListReturnType::INDEX }

    it "returns the index of the specified value" do
      operation = ListOperation.get_by_value(list_bin, 2)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to contain_exactly(2, 6)
    end
  end

  describe "ListOperation.get_by_value_range" do
    let(:list_value) { [1, 4, 2, 3, 5, 1, 2] }
    let(:return_type) { ListReturnType::INDEX }

    it "returns the indeces of the items in the specified value range" do
      operation = ListOperation.get_by_value_range(list_bin, 2, 4)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to contain_exactly(2, 3, 6)
    end

    it "returns the indeces of the items starting with the specified value" do
      operation = ListOperation.get_by_value_range(list_bin, 2)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to contain_exactly(1, 2, 3, 4, 6)
    end
  end

  describe "ListOperation.get_by_value_list" do
    let(:list_value) { [1, 4, 2, 3, 5, 1, 2] }
    let(:return_type) { ListReturnType::INDEX }

    it "returns the indeces of the items in the specified list" do
      operation = ListOperation.get_by_value_list(list_bin, [2, 4])
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to contain_exactly(1, 2, 6)
    end
  end

  describe "ListOperation.get_by_value_rel_rank_range" do
    let(:list_value) { [0, 4, 5, 9, 11, 15] }

    it "returns the values of the items nearest to and greater than the specified value, by relative rank range" do
      operation = ListOperation.get_by_value_rel_rank_range(list_bin, 5, 0, 2)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to contain_exactly(5, 9)
    end

    it "returns the values of the items nearest to and greater than the specified value, starting with the specified relative rank" do
      operation = ListOperation.get_by_value_rel_rank_range(list_bin, 5, 0)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to contain_exactly(5, 9, 11, 15)
    end
  end

  describe "ListOperation.remove_by_index" do
    let(:list_value) { [1, 2, 3, 4, 5] }

    it "removes the value at the specified index" do
      operation = ListOperation.remove_by_index(list_bin, 2)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 3
      expect(list_post_op).to eql([1, 2, 4, 5])
    end

    it "returns an error if the index is out of bounds" do
      operation = ListOperation.remove_by_index(list_bin, 99)

      expect { client.operate(key, [operation]) }.to raise_error(/Parameter error/)
    end
  end

  describe "ListOperation.remove_by_index_range" do
    let(:list_value) { [1, 2, 3, 4, 5] }

    it "removes the values at the specified index range" do
      operation = ListOperation.remove_by_index_range(list_bin, 1, 3)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to eql([2, 3, 4])
      expect(list_post_op).to eql([1, 5])
    end

    it "returns all values starting at the specified index if count is not specified" do
      operation = ListOperation.remove_by_index_range(list_bin, 1)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to eql([2, 3, 4, 5])
      expect(list_post_op).to eql([1])
    end
  end

  describe "ListOperation.remove_by_rank" do
    let(:list_value) { [3, 4, 1, 5, 2] }

    it "removes the value at the specified rank" do
      operation = ListOperation.remove_by_rank(list_bin, 0)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 1
      expect(list_post_op).to eql([3, 4, 5, 2])
    end
  end

  describe "ListOperation.remove_by_rank_range" do
    let(:list_value) { [3, 4, 1, 5, 2] }

    it "removes the value at the specified rank range" do
      operation = ListOperation.remove_by_rank_range(list_bin, 1, 3)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to contain_exactly(2, 3, 4)
      expect(list_post_op).to eql([1, 5])
    end

    it "returns all values starting at the specified index if count is not specified" do
      operation = ListOperation.remove_by_rank_range(list_bin, 3)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to contain_exactly(4, 5)
      expect(list_post_op).to eql([3, 1, 2])
    end
  end

  describe "ListOperation.remove_by_value" do
    let(:list_value) { [1, 4, 2, 3, 5, 1, 2] }
    let(:return_type) { ListReturnType::INDEX }

    it "removes the index of the specified value" do
      operation = ListOperation.remove_by_value(list_bin, 2)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to contain_exactly(2, 6)
      expect(list_post_op).to eql([1, 4, 3, 5, 1])
    end
  end

  describe "ListOperation.remove_by_value_range" do
    let(:list_value) { [1, 4, 2, 3, 5, 1, 2] }
    let(:return_type) { ListReturnType::INDEX }

    it "removes the indeces of the items in the specified value range" do
      operation = ListOperation.remove_by_value_range(list_bin, 2, 4)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to contain_exactly(2, 3, 6)
      expect(list_post_op).to eql([1, 4, 5, 1])
    end

    it "removes the indeces of the items starting with the specified value" do
      operation = ListOperation.remove_by_value_range(list_bin, 2)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to contain_exactly(1, 2, 3, 4, 6)
      expect(list_post_op).to eql([1, 1])
    end
  end

  describe "ListOperation.remove_by_value_list" do
    let(:list_value) { [1, 4, 2, 3, 5, 1, 2] }
    let(:return_type) { ListReturnType::INDEX }

    it "removes the indeces of the items in the specified list" do
      operation = ListOperation.remove_by_value_list(list_bin, [2, 4])
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to contain_exactly(1, 2, 6)
      expect(list_post_op).to eql([1, 3, 5, 1])
    end
  end

  describe "ListOperation.remove_by_value_rel_rank_range" do
    let(:list_value) { [0, 4, 5, 9, 11, 15] }

    it "removes the values of the items nearest to and greater than the specified value, by relative rank range" do
      operation = ListOperation.remove_by_value_rel_rank_range(list_bin, 5, 0, 2)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to contain_exactly(5, 9)
      expect(list_post_op).to eql([0, 4, 11, 15])
    end

    it "removes the values of the items nearest to and greater than the specified value, starting with the specified relative rank" do
      operation = ListOperation.remove_by_value_rel_rank_range(list_bin, 5, 0)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to contain_exactly(5, 9, 11, 15)
      expect(list_post_op).to eql([0, 4])
    end
  end

  describe "ListOperation#and_return" do
    let(:list_value) { [1, 4, 2, 3, 5, 1, 2] }

    it "returns nothing by default" do
      operation = ListOperation.remove_by_index_range(list_bin, 2, 4)
        .and_return(ListReturnType::DEFAULT)
      result = client.operate(key, [operation])

      expect(result.bins).to be nil
    end

    it "returns the list index" do
      operation = ListOperation.remove_by_index_range(list_bin, 2, 4)
        .and_return(ListReturnType::INDEX)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to eql([2, 3, 4, 5])
    end

    it "returns the reverse list index" do
      operation = ListOperation.remove_by_index_range(list_bin, 2, 4)
        .and_return(ListReturnType::REVERSE_INDEX)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to eql([1, 2, 3, 4])
    end

    it "returns the list rank" do
      operation = ListOperation.remove_by_index_range(list_bin, 2, 4)
        .and_return(ListReturnType::RANK)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to eql([2, 4, 6, 0])
    end

    it "returns the reverse list rank" do
      operation = ListOperation.remove_by_index_range(list_bin, 2, 4)
        .and_return(ListReturnType::REVERSE_RANK)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to eql([4, 2, 0, 6])
    end

    it "returns the number of items" do
      operation = ListOperation.remove_by_index_range(list_bin, 2, 4)
        .and_return(ListReturnType::COUNT)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to be 4
    end

    it "returns the value of the items" do
      operation = ListOperation.remove_by_index_range(list_bin, 2, 4)
        .and_return(ListReturnType::VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to eql([2, 3, 5, 1])
    end
  end

  describe "InvertibleListOp#invert_selection" do
    let(:list_value) { [1, 4, 2, 3, 5, 1, 2] }

    it "inverts the selection of items affected by the operation" do
      operation = ListOperation.remove_by_index_range(list_bin, 2, 4)
        .and_return(return_type)
        .invert_selection
      result = client.operate(key, [operation])

      expect(result.bins[list_bin]).to eql([1, 4, 2])
      expect(list_post_op).to eql([2, 3, 5, 1])
    end
  end

  describe "ListPolicy" do

    context "ordered list" do
      let(:list_value) { [5, 4, 3, 2, 1] }
      let(:order) { ListOrder::ORDERED }

      it "creates an ordered list" do
        expect(list_post_op).to eql([1, 2, 3, 4, 5])
      end
    end

    context "with add-unique flag" do
      let(:list_value) { [1, 2, 3, 4, 5] }
      let(:write_flags) { ListWriteFlags::ADD_UNIQUE }

      it "throws an error when trying to insert a non-unique item" do
        operation = ListOperation.append(list_bin, 3, 6, policy: list_policy)

        expect { client.operate(key, [operation]) }.to raise_error(/Element already exists/)
      end

      context "with no-fail flag" do
        let(:write_flags) { ListWriteFlags::ADD_UNIQUE | ListWriteFlags::NO_FAIL }

        it "does not modify the list but returns ok" do
          operation = ListOperation.append(list_bin, 3, 6, policy: list_policy)
          result = client.operate(key, [operation])

          expect(result.bins[list_bin]).to be 5
          expect(list_post_op).to eql([1, 2, 3, 4, 5])
        end

        context "with partial flag" do
          let(:write_flags) { ListWriteFlags::ADD_UNIQUE | ListWriteFlags::NO_FAIL | ListWriteFlags::PARTIAL }

          it "appends only the unique items" do
            operation = ListOperation.append(list_bin, 3, 6, policy: list_policy)
            result = client.operate(key, [operation])

            expect(result.bins[list_bin]).to be 6
            expect(list_post_op).to eql([1, 2, 3, 4, 5, 6])
          end
        end
      end
    end

    context "with insert-bounded flag" do
      let(:list_value) { [1, 2, 3, 4, 5] }
      let(:write_flags) { ListWriteFlags::INSERT_BOUNED }

      it "allows inserts inside the existing list boundaries" do
        operation = ListOperation.insert(list_bin, 3, 6, policy: list_policy)

        expect { client.operate(key, [operation]) }.not_to raise_error
        expect(list_post_op).to eql([1, 2, 3, 6, 4, 5])
      end

      it "throws an error when trying to insert an item outside the existing list boundaries" do
        operation = ListOperation.insert(list_bin, 99, 6, policy: list_policy)

        expect { client.operate(key, [operation]) }.to raise_error(/Parameter error/)
      end
    end
  end
end
