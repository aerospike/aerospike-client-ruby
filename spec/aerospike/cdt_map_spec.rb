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
  let(:map_bin) { "map" }
  let(:map_value) { }
  let(:return_type) { MapReturnType::VALUE }
  let(:map_order) { MapOrder::UNORDERED }
  let(:write_mode) { MapWriteMode::UPDATE }
  let(:map_policy) { MapPolicy.new(order: map_order, write_mode: write_mode) }

  before(:each) do
    next unless map_value

    case map_order
    when MapOrder::UNORDERED
      client.put(key, { map_bin => map_value })
    else
      # Use put_items op to create map so that we can control map order.
      create_policy = MapPolicy.new(
        order: map_policy.order,
        write_mode: MapWriteMode::CREATE_ONLY
      )
      op = MapOperation.put_items(map_bin, map_value, policy: create_policy)
      client.operate(key, [op])
    end
  end

  def map_post_op
    client.get(key).bins[map_bin]
  end

  describe "MapOperation.set_policy" do
    let(:map_value) { { "c" => 1, "b" => 2, "a" => 3 } }
    let(:map_order) { MapOrder::UNORDERED }

    it "changes the map order" do
      new_policy = MapPolicy.new(order: MapOrder::KEY_ORDERED)
      operation = MapOperation.set_policy(map_bin, new_policy)
      client.operate(key, [operation])

      expect(map_post_op.to_a).to eql([["a", 3], ["b", 2], ["c", 1]])
    end
  end

  describe "MapOperation.put" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "adds the item to the map and returns the map size" do
      operation = MapOperation.put(map_bin, "x", 99)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql(4)
      expect(map_post_op).to eql({ "a" => 1, "b" => 2, "c" => 3, "x" => 99 })
    end

  end

  describe "MapOperation.put_items" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "adds the items to the map and returns the map size" do
      operation = MapOperation.put_items(map_bin, { "x" => 99 })
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql(4)
      expect(map_post_op).to eql({ "a" => 1, "b" => 2, "c" => 3, "x" => 99 })
    end
  end

  describe "MapOperation.increment" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "increments the value for all items identified by the key and returns the final result" do
      operation = MapOperation.increment(map_bin, "b", 3)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql(5)
      expect(map_post_op).to eql({ "a" => 1, "b" => 5, "c" => 3 })
    end
  end

  describe "MapOperation.decrement" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "decrements the value for all items identified by the key and returns the final result" do
      operation = MapOperation.decrement(map_bin, "b", 3)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql(-1)
      expect(map_post_op).to eql({ "a" => 1, "b" => -1, "c" => 3 })
    end
  end

  describe "MapOperation.clear" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "removes all items from the map" do
      operation = MapOperation.clear(map_bin)
      result = client.operate(key, [operation])

      expect(result.bins).to be_nil
      expect(map_post_op).to eql({ })
    end
  end

  describe "MapOperation.remove_keys" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "removes a single key from the map" do
      operation = MapOperation.remove_keys(map_bin, "b")
      result = client.operate(key, [operation])

      expect(result.bins).to be_nil
      expect(map_post_op).to eql({ "a" => 1, "c" => 3 })
    end

    it "removes a list of keys from the map" do
      operation = MapOperation.remove_keys(map_bin, "a", "b")
      result = client.operate(key, [operation])

      expect(result.bins).to be_nil
      expect(map_post_op).to eql({ "c" => 3 })
    end
  end

  describe "MapOperation.remove_key_range" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "removes the specified key range from the map" do
      operation = MapOperation.remove_key_range(map_bin, "b", "c")
      result = client.operate(key, [operation])

      expect(result.bins).to be_nil
      expect(map_post_op).to eql({ "a" => 1, "c" => 3 })
    end

    it "removes the all keys from the specified start key until the end" do
      operation = MapOperation.remove_key_range(map_bin, "b")
      result = client.operate(key, [operation])

      expect(result.bins).to be_nil
      expect(map_post_op).to eql({ "a" => 1 })
    end

    it "removes the all keys until the specified end key" do
      operation = MapOperation.remove_key_range(map_bin, nil, "b")
      result = client.operate(key, [operation])

      expect(result.bins).to be_nil
      expect(map_post_op).to eql({ "b" => 2, "c" => 3 })
    end
  end

  describe "MapOperation.remove_values" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3, "d" => 2 } }

    it "removes the items identified by a single value" do
      operation = MapOperation.remove_values(map_bin, 2)
      result = client.operate(key, [operation])

      expect(result.bins).to be_nil
      expect(map_post_op).to eql({ "a" => 1, "c" => 3 })
    end

    it "removes the items identified by a list of values" do
      operation = MapOperation.remove_values(map_bin, 2, 3)
      result = client.operate(key, [operation])

      expect(result.bins).to be_nil
      expect(map_post_op).to eql({ "a" => 1 })
    end
  end

  describe "MapOperation.remove_value_range" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "removes the specified value range from the map" do
      operation = MapOperation.remove_value_range(map_bin, 2, 3)
      result = client.operate(key, [operation])

      expect(result.bins).to be_nil
      expect(map_post_op).to eql({ "a" => 1, "c" => 3 })
    end

    it "removes all elements starting from the specified start value until the end" do
      operation = MapOperation.remove_value_range(map_bin, 2)
      result = client.operate(key, [operation])

      expect(result.bins).to be_nil
      expect(map_post_op).to eql({ "a" => 1 })
    end

    it "removes all elements until the specified end value" do
      operation = MapOperation.remove_value_range(map_bin, nil, 3)
      result = client.operate(key, [operation])

      expect(result.bins).to be_nil
      expect(map_post_op).to eql({ "c" => 3 })
    end
  end

  describe "MapOperation.remove_index" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "removes a map item identified by index from the map" do
      operation = MapOperation.remove_index(map_bin, 1)
      result = client.operate(key, [operation])

      expect(result.bins).to be_nil
      expect(map_post_op).to eql({ "a" => 1, "c" => 3 })
    end
  end

  describe "MapOperation.remove_index_range" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "removes 'count' map items starting at the specified index from the map" do
      operation = MapOperation.remove_index_range(map_bin, 1, 2)
      result = client.operate(key, [operation])

      expect(result.bins).to be_nil
      expect(map_post_op).to eql({ "a" => 1 })
    end

    it "removes all items starting at the specified index to the end of the map" do
      operation = MapOperation.remove_index_range(map_bin, 1)
      result = client.operate(key, [operation])

      expect(result.bins).to be_nil
      expect(map_post_op).to eql({ "a" => 1 })
    end
  end

  describe "MapOperation.remove_by_rank" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "removes a map item identified by rank from the map" do
      operation = MapOperation.remove_by_rank(map_bin, 1)
      result = client.operate(key, [operation])

      expect(result.bins).to be_nil
      expect(map_post_op).to eql({ "a" => 1, "c" => 3 })
    end
  end

  describe "MapOperation.remove_by_rank_range" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "removes 'count' map items starting at the specified rank from the map" do
      operation = MapOperation.remove_by_rank_range(map_bin, 1, 2)
      result = client.operate(key, [operation])

      expect(result.bins).to be_nil
      expect(map_post_op).to eql({ "a" => 1 })
    end

    it "removes all items starting at the specified rank to the end of the map" do
      operation = MapOperation.remove_by_rank_range(map_bin, 1)
      result = client.operate(key, [operation])

      expect(result.bins).to be_nil
      expect(map_post_op).to eql({ "a" => 1 })
    end
  end

  describe "MapOperation.size" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "returns the size of the map" do
      operation = MapOperation.size(map_bin)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql(3)
    end
  end

  describe "MapOperation.get_key" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }
    let(:return_type) { MapReturnType::KEY_VALUE }

    it "gets a single key from the map" do
      operation = MapOperation.get_key(map_bin, "b")
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "b" => 2 })
    end
  end

  describe "MapOperation.get_key_range" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }
    let(:return_type) { MapReturnType::KEY_VALUE }

    it "gets the specified key range from the map" do
      operation = MapOperation.get_key_range(map_bin, "b", "c")
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "b" => 2 })
    end

    it "gets all keys from the specified start key until the end" do
      operation = MapOperation.get_key_range(map_bin, "b")
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "b" => 2, "c" => 3 })
    end

    it "gets all keys from the start to the specified end key" do
      operation = MapOperation.get_key_range(map_bin, nil, "b")
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "a" => 1 })
    end
  end

  describe "MapOperation.get_value" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3, "d" => 2 } }
    let(:return_type) { MapReturnType::KEY_VALUE }

    it "gets the item identified by a single value" do
      operation = MapOperation.get_value(map_bin, 2)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "b" => 2, "d" => 2 })
    end
  end

  describe "MapOperation.get_value_range" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3, "d" => 2} }
    let(:return_type) { MapReturnType::KEY_VALUE }

    it "gets the specified key range from the map" do
      operation = MapOperation.get_value_range(map_bin, 2, 3)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "b" => 2, "d" => 2 })
    end

    it "gets all values from the specified start value until the end" do
      operation = MapOperation.get_value_range(map_bin, 2)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "b" => 2, "d" => 2, "c" => 3 })
    end

    it "gets all values from the start of the map until the specified end value" do
      operation = MapOperation.get_value_range(map_bin, nil, 3)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "a" => 1, "b" => 2, "d" => 2 })
    end
  end

  describe "MapOperation.get_index" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }
    let(:return_type) { MapReturnType::KEY_VALUE }

    it "gets a map item identified by index from the map" do
      operation = MapOperation.get_index(map_bin, 1)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "b" => 2 })
    end
  end

  describe "MapOperation.get_index_range" do
    let(:map_value) { { "c" => 1, "b" => 2, "a" => 3 } }
    let(:return_type) { MapReturnType::KEY }

    it "gets 'count' map items starting at the specified index from the map" do
      operation = MapOperation.get_index_range(map_bin, 1, 2)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql([ "b", "c" ])
    end

    it "gets all items starting at the specified index to the end of the map" do
      operation = MapOperation.get_index_range(map_bin, 1)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql([ "b", "c" ])
    end
  end

  describe "MapOperation.get_by_rank" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }
    let(:return_type) { MapReturnType::KEY_VALUE }

    it "gets a map item identified by rank from the map" do
      operation = MapOperation.get_by_rank(map_bin, 1)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "b" => 2 })
    end
  end

  describe "MapOperation.get_by_rank_range" do
    let(:map_value) { { "a" => 3, "b" => 2, "c" => 1 } }
    let(:return_type) { MapReturnType::KEY }

    it "gets 'count' map items starting at the specified rank from the map" do
      operation = MapOperation.get_by_rank_range(map_bin, 1, 2)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql([ "b", "a" ])
    end

    it "gets all items starting at the specified rank to the end of the map" do
      operation = MapOperation.get_by_rank_range(map_bin, 1)
        .and_return(return_type)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql([ "b", "a" ])
    end
  end

  context "MapReturnType" do
    let(:map_value) { { "a" => 3, "b" => 2, "c" => 1 } }
    let(:map_order) { MapOrder::KEY_ORDERED }

    context "NONE" do
      it "returns nothing" do
        operation = MapOperation.get_key(map_bin, "a")
          .and_return(MapReturnType::NONE)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to eql(nil)
      end
    end

    context "INDEX" do
      it "returns key index" do
        operation = MapOperation.get_key(map_bin, "a")
          .and_return(MapReturnType::INDEX)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to eql(0)
      end
    end

    context "REVERSE_INDEX" do
      it "returns reverse key index" do
        operation = MapOperation.get_key(map_bin, "a")
          .and_return(MapReturnType::REVERSE_INDEX)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to eql(2)
      end
    end

    context "RANK" do
      it "returns value order (rank)" do
        operation = MapOperation.get_key(map_bin, "a")
          .and_return(MapReturnType::RANK)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to eql(2)
      end
    end

    context "REVERSE_RANK" do
      it "returns reverse value order (reverse rank)" do
        operation = MapOperation.get_key(map_bin, "a")
          .and_return(MapReturnType::REVERSE_RANK)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to eql(0)
      end
    end

    context "COUNT" do
      it "returns count of items selected" do
        operation = MapOperation.get_key_range(map_bin, "a", "c")
          .and_return(MapReturnType::COUNT)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to eql(2)
      end
    end

    context "KEY" do
      it "returns the map key" do
        operation = MapOperation.get_index(map_bin, 0)
          .and_return(MapReturnType::KEY)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to eql("a")
      end
    end

    context "VALUE" do
      it "returns the map value" do
        operation = MapOperation.get_index(map_bin, 0)
          .and_return(MapReturnType::VALUE)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to eql(3)
      end
    end

    context "KEY_VALUE" do
      it "returns the map key & value" do
        operation = MapOperation.get_index(map_bin, 0)
          .and_return(MapReturnType::KEY_VALUE)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to eql({ "a" => 3 })
      end
    end
  end

  context "MapOrder" do
    let(:map_value) { { "c" => 1, "b" => 2, "a" => 3 } }
    let(:ordered_key_values) do
      ops = [
        MapOperation.set_policy(map_bin, map_policy),
        MapOperation.get_by_key_range(map_bin, "a")
          .and_return(MapReturnType::KEY_VALUE)
      ]
      result = client.operate(key, ops)
      result.bins[map_bin].to_a
    end

    context "UNORDERED" do
      let(:map_order) { MapOrder::UNORDERED }
      it { expect(ordered_key_values).to eql([["c", 1], ["b", 2], ["a", 3]]) }
    end

    context "KEY_ORDERED" do
      let(:map_order) { MapOrder::KEY_ORDERED }
      it { expect(ordered_key_values).to eql([["a", 3], ["b", 2], ["c", 1]]) }
    end

    context "KEY_VALUE_ORDERED" do
      let(:map_order) { MapOrder::KEY_VALUE_ORDERED }
      it { expect(ordered_key_values).to eql([["a", 3], ["b", 2], ["c", 1]]) }
    end
  end

  context "MapWriteMode" do
    let(:operation) { MapOperation.put(map_bin, "b", 99, policy: map_policy) }

    context "UPDATE" do
      let(:write_mode) { MapWriteMode::UPDATE }

      context "map does not exist" do
        it "creates a new map" do
          client.operate(key, [operation])

          expect(map_post_op).to eql({ "b" => 99 })
        end
      end

      context "map exists" do
        context "element exists" do
          let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

          it "updates the element" do
            client.operate(key, [operation])

            expect(map_post_op).to eql({ "a" => 1, "b" => 99, "c" => 3 })
          end
        end

        context "element does not exist" do
          let(:map_value) { { "a" => 1, "c" => 3 } }

          it "creates the element" do
            client.operate(key, [operation])

            expect(map_post_op).to eql({ "a" => 1, "b" => 99, "c" => 3 })
          end
        end
      end
    end

    context "UPDATE_ONLY" do
      let(:write_mode) { MapWriteMode::UPDATE_ONLY }

      context "map does not exist" do
        it "returns an error" do
          expect { client.operate(key, [operation]) }.to raise_error(/Element not found/)
        end
      end

      context "map exists" do
        context "element exists" do
          let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

          it "updates the element" do
            client.operate(key, [operation])

            expect(map_post_op).to eql({ "a" => 1, "b" => 99, "c" => 3 })
          end
        end

        context "element does not exist" do
          let(:map_value) { { "a" => 1, "c" => 3 } }

          it "returns an error" do
            expect { client.operate(key, [operation]) }.to raise_error(/Element not found/)
          end
        end
      end
    end

    context "CREATE_ONLY" do
      let(:write_mode) { MapWriteMode::CREATE_ONLY }

      context "map does not exist" do
        it "creates a new map" do
          client.operate(key, [operation])

          expect(map_post_op).to eql({ "b" => 99 })
        end
      end

      context "map exists" do
        context "element exists" do
          let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

          it "returns an error" do
            expect { client.operate(key, [operation]) }.to raise_error(/Element already exists/)
          end
        end

        context "element does not exist" do
          let(:map_value) { { "a" => 1, "c" => 3 } }

          it "adds the element" do
            client.operate(key, [operation])

            expect(map_post_op).to eql({ "a" => 1, "b" => 99, "c" => 3 })
          end
        end
      end
    end
  end
end
