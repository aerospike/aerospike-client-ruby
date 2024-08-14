# encoding: utf-8
# Copyright 2014-2020 Aerospike, Inc.
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

include Aerospike
include Aerospike::CDT

describe "client.operate() - CDT Map Operations", skip: !Support.feature?(Aerospike::Features::CDT_MAP) do
  let(:client) { Support.client }
  let(:key) { Support.gen_random_key }
  let(:map_bin) { "map_bin" }
  let(:map_value) { nil }

  before(:each) do
    next unless map_value

    create_policy = WritePolicy.new(record_exists_action: RecordExistsAction::CREATE_ONLY)
    client.put(key, { map_bin => map_value }, create_policy)
  end

  def map_post_op
    client.get(key).bins[map_bin]
  end

  describe "MapOperation Context", skip: !Support.min_version?("4.6") do
    it "should support Create Map ops" do
      client.delete(key)

      m = {
        "key1" => [7, 9, 5]
      }

      client.put(key, Aerospike::Bin.new(map_bin, m))
      expect(map_post_op).to eq(m)

      ctx = [Context.map_key("key2")]
      record = client.operate(key,
                              [
                                ListOperation.create(map_bin, ListOrder::ORDERED, false, ctx: ctx),
                                ListOperation.append(map_bin, 2, ctx: ctx),
                                ListOperation.append(map_bin, 1, ctx: ctx),
                                Operation.get(map_bin)
                              ])

      expect(record.bins[map_bin]).to eq({
                                           "key1" => [7, 9, 5],
                                           "key2" => [1, 2]
                                         })
    end

    it "should support Nested Map ops with Lists" do
      client.delete(key)

      m = {
        "key1" => {
          "key11" => 9, "key12" => 4
        },
        "key2" => {
          "key21" => 3, "key22" => 5
        }
      }

      client.put(key, Aerospike::Bin.new(map_bin, m))
      record = client.operate(key, [Aerospike::Operation.get(map_bin)])
      expect(record.bins[map_bin]).to eq(m)

      record = client.operate(key, [MapOperation.put(map_bin, "key21", 11, ctx: [Context.map_key("key2")]), Aerospike::Operation.get(map_bin)])
      expect(record.bins[map_bin]).to eq({
                                           "key1" => {
                                             "key11" => 9, "key12" => 4
                                           },
                                           "key2" => {
                                             "key21" => 11, "key22" => 5
                                           }
                                         })
    end

    it "should support Nested Map ops" do
      client.delete(key)

      m = {
        "key1" => {
          "key11" => 9, "key12" => 4
        },
        "key2" => {
          "key21" => 3, "key22" => 5
        }
      }

      client.put(key, Aerospike::Bin.new(map_bin, m))
      record = client.operate(key, [Aerospike::Operation.get(map_bin)])
      expect(record.bins[map_bin]).to eq(m)

      record = client.operate(key, [MapOperation.put(map_bin, "key21", 11, ctx: [Context.map_key("key2")]), Aerospike::Operation.get(map_bin)])
      expect(record.bins[map_bin]).to eq({
                                           "key1" => {
                                             "key11" => 9, "key12" => 4
                                           },
                                           "key2" => {
                                             "key21" => 11, "key22" => 5
                                           }
                                         })
    end

    it "should support Double Nested Map ops" do
      client.delete(key)

      m = {
        "key1" => {
          "key11" => { "key111" => 1 }, "key12" => { "key121" => 5 }
        },
        "key2" => {
          "key21" => { "key211" => 7 }
        }
      }

      client.put(key, Aerospike::Bin.new(map_bin, m))

      record = client.operate(key, [Aerospike::Operation.get(map_bin)])
      expect(record.bins[map_bin]).to eq(m)

      record = client.operate(key, [MapOperation.put(map_bin, "key121", 11, ctx: [Context.map_key("key1"), Context.map_rank(-1)]), Aerospike::Operation.get(map_bin)])

      expect(record.bins[map_bin]).to eq({
                                           "key1" => {
                                             "key11" => { "key111" => 1 }, "key12" => { "key121" => 11 }
                                           },
                                           "key2" => {
                                             "key21" => { "key211" => 7 }
                                           }
                                         })
    end
  end

  describe "MapOperation.set_policy" do
    let(:map_value) { { "c" => 1, "b" => 2, "a" => 3 } }

    it "sets the map order" do
      new_policy = MapPolicy.new(order: MapOrder::KEY_ORDERED, persist_index: true)
      operation = MapOperation.set_policy(map_bin, new_policy)

      expect { client.operate(key, [operation]) }.not_to raise_error
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

      expected = { map_bin => nil }
      expect(result.bins).to eq expected
      expect(map_post_op).to eql({})
    end
  end

  describe "MapOperation.remove_by_key" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "removes a single key from the map" do
      operation = MapOperation.remove_by_key(map_bin, "b")
                              .and_return(MapReturnType::VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to be(2)
      expect(map_post_op).to eql({ "a" => 1, "c" => 3 })
    end
  end

  describe "MapOperation.remove_by_key_list" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "removes a list of keys from the map" do
      operation = MapOperation.remove_by_key_list(map_bin, %w[a b])
                              .and_return(MapReturnType::VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to contain_exactly(1, 2)
      expect(map_post_op).to eql({ "c" => 3 })
    end
  end

  describe "MapOperation.remove_by_key_range" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "removes the specified key range from the map" do
      operation = MapOperation.remove_by_key_range(map_bin, "b", "c")
      result = client.operate(key, [operation])

      expected = { map_bin => nil }
      expect(result.bins).to eq expected
      expect(map_post_op).to eql({ "a" => 1, "c" => 3 })
    end

    it "removes the all keys from the specified start key until the end" do
      operation = MapOperation.remove_by_key_range(map_bin, "b")
      result = client.operate(key, [operation])

      expected = { map_bin => nil }
      expect(result.bins).to eq expected
      expect(map_post_op).to eql({ "a" => 1 })
    end

    it "removes the all keys until the specified end key" do
      operation = MapOperation.remove_by_key_range(map_bin, nil, "b")
      result = client.operate(key, [operation])

      expected = { map_bin => nil }
      expect(result.bins).to eq expected
      expect(map_post_op).to eql({ "b" => 2, "c" => 3 })
    end
  end

  describe "MapOperation.remove_by_key_rel_index_range", skip: !Support.min_version?("4.3") do
    let(:map_value) { { "a" => 17, "e" => 2, "f" => 15, "j" => 10 } }

    it "removes specified number of elements" do
      operation = MapOperation.remove_by_key_rel_index_range(map_bin, "f", 1, 2)
                              .and_return(MapReturnType::KEY)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to contain_exactly("j")
      expect(map_post_op).to eql({ "a" => 17, "e" => 2, "f" => 15 })
    end

    it "removes elements from specified key until the end" do
      operation = MapOperation.remove_by_key_rel_index_range(map_bin, "f", 1)
                              .and_return(MapReturnType::KEY)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to contain_exactly("j")
      expect(map_post_op).to eql({ "a" => 17, "e" => 2, "f" => 15 })
    end
  end

  describe "MapOperation.remove_by_value" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3, "d" => 2 } }

    it "removes the items identified by a single value" do
      operation = MapOperation.remove_by_value(map_bin, 2)
                              .and_return(MapReturnType::KEY)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql(%w[b d])
      expect(map_post_op).to eql({ "a" => 1, "c" => 3 })
    end
  end

  describe "MapOperation.remove_by_value_list" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3, "d" => 2 } }

    it "removes the items identified by a list of values" do
      operation = MapOperation.remove_by_value_list(map_bin, [2, 3])
                              .and_return(MapReturnType::KEY)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to contain_exactly("b", "c", "d")
      expect(map_post_op).to eql({ "a" => 1 })
    end
  end

  describe "MapOperation.remove_by_value_range" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "removes the specified value range from the map" do
      operation = MapOperation.remove_by_value_range(map_bin, 2, 3)
      result = client.operate(key, [operation])

      expected = { map_bin => nil }
      expect(result.bins).to eq expected
      expect(map_post_op).to eql({ "a" => 1, "c" => 3 })
    end

    it "removes all elements starting from the specified start value until the end" do
      operation = MapOperation.remove_by_value_range(map_bin, 2)
      result = client.operate(key, [operation])

      expected = { map_bin => nil }
      expect(result.bins).to eq expected
      expect(map_post_op).to eql({ "a" => 1 })
    end

    it "removes all elements until the specified end value" do
      operation = MapOperation.remove_by_value_range(map_bin, nil, 3)
      result = client.operate(key, [operation])

      expected = { map_bin => nil }
      expect(result.bins).to eq expected
      expect(map_post_op).to eql({ "c" => 3 })
    end
  end

  describe "MapOperation.remove_by_value_rel_rank_range", skip: !Support.min_version?("4.3") do
    let(:map_value) { { 4 => 2, 9 => 10, 5 => 15, 0 => 17 } }

    it "removes specified number of elements" do
      operation = MapOperation.remove_by_value_rel_rank_range(map_bin, 11, -1, 1)
                              .and_return(MapReturnType::KEY_VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ 9 => 10 })
      expect(map_post_op).to eql({ 4 => 2, 5 => 15, 0 => 17 })
    end

    it "removes elements from specified key until the end" do
      operation = MapOperation.remove_by_value_rel_rank_range(map_bin, 11, -1)
                              .and_return(MapReturnType::KEY_VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ 9 => 10, 5 => 15, 0 => 17 })
      expect(map_post_op).to eql({ 4 => 2 })
    end
  end

  describe "MapOperation.remove_by_index" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "removes a map item identified by index from the map" do
      operation = MapOperation.remove_by_index(map_bin, 1)
      result = client.operate(key, [operation])

      expected = { map_bin => nil }
      expect(result.bins).to eq expected
      expect(map_post_op).to eql({ "a" => 1, "c" => 3 })
    end
  end

  describe "MapOperation.remove_by_index_range" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "removes 'count' map items starting at the specified index from the map" do
      operation = MapOperation.remove_by_index_range(map_bin, 1, 2)
      result = client.operate(key, [operation])

      expected = { map_bin => nil }
      expect(result.bins).to eq expected
      expect(map_post_op).to eql({ "a" => 1 })
    end

    it "removes all items starting at the specified index to the end of the map" do
      operation = MapOperation.remove_by_index_range(map_bin, 1)
      result = client.operate(key, [operation])

      expected = { map_bin => nil }
      expect(result.bins).to eq expected
      expect(map_post_op).to eql({ "a" => 1 })
    end
  end

  describe "MapOperation.remove_by_rank" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "removes a map item identified by rank from the map" do
      operation = MapOperation.remove_by_rank(map_bin, 1)
      result = client.operate(key, [operation])

      expected = { map_bin => nil }
      expect(result.bins).to eq expected
      expect(map_post_op).to eql({ "a" => 1, "c" => 3 })
    end
  end

  describe "MapOperation.remove_by_rank_range" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "removes 'count' map items starting at the specified rank from the map" do
      operation = MapOperation.remove_by_rank_range(map_bin, 1, 2)
      result = client.operate(key, [operation])

      expected = { map_bin => nil }
      expect(result.bins).to eq expected
      expect(map_post_op).to eql({ "a" => 1 })
    end

    it "removes all items starting at the specified rank to the end of the map" do
      operation = MapOperation.remove_by_rank_range(map_bin, 1)
      result = client.operate(key, [operation])

      expected = { map_bin => nil }
      expect(result.bins).to eq expected
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

  describe "MapOperation.get_by_key" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "gets a single key from the map" do
      operation = MapOperation.get_by_key(map_bin, "b")
                              .and_return(MapReturnType::KEY_VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "b" => 2 })
    end
  end

  describe "MapOperation.get_by_key_list" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "gets a list of keys from the map" do
      operation = MapOperation.get_by_key_list(map_bin, %w[b c])
                              .and_return(MapReturnType::VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to contain_exactly(2, 3)
    end
  end

  describe "MapOperation.get_by_key_range" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "gets the specified key range from the map" do
      operation = MapOperation.get_by_key_range(map_bin, "b", "c")
                              .and_return(MapReturnType::KEY_VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "b" => 2 })
    end

    it "gets all keys from the specified start key until the end" do
      operation = MapOperation.get_by_key_range(map_bin, "b")
                              .and_return(MapReturnType::KEY_VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "b" => 2, "c" => 3 })
    end

    it "gets all keys from the start to the specified end key" do
      operation = MapOperation.get_by_key_range(map_bin, nil, "b")
                              .and_return(MapReturnType::KEY_VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "a" => 1 })
    end
  end

  describe "MapOperation.get_by_key_range" do
    let(:map_value) { { "b" => 2, "a" => 1, "c" => 3 } }

    it "gets all keys from the start to the specified end key, return ordered map" do
      operation = MapOperation.get_by_key_range(map_bin, nil, "c")
                              .and_return(MapReturnType::ORDERED_MAP)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "a" => 1, "b" => 2 })
    end

    it "gets all keys from the start to the specified end key, return unordered map" do
      operation = MapOperation.get_by_key_range(map_bin, nil, "c")
                              .and_return(MapReturnType::UNORDERED_MAP)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "b" => 2, "a" => 1 })
    end
  end

  describe "MapOperation.get_by_key_rel_index_range", skip: !Support.min_version?("4.3") do
  let(:map_value) { { "a" => 17, "e" => 2, "f" => 15, "j" => 10 } }

  it "gets specified number of elements" do
    operation = MapOperation.get_by_key_rel_index_range(map_bin, "f", 1, 2)
                            .and_return(MapReturnType::KEY)
    result = client.operate(key, [operation])

    expect(result.bins[map_bin]).to contain_exactly("j")
  end

  it "get elements from specified key until the end" do
    operation = MapOperation.get_by_key_rel_index_range(map_bin, "f", 1)
                            .and_return(MapReturnType::KEY)
    result = client.operate(key, [operation])

    expect(result.bins[map_bin]).to contain_exactly("j")
  end
  end

  describe "MapOperation.get_by_value" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3, "d" => 2 } }

    it "gets the item identified by a single value" do
      operation = MapOperation.get_by_value(map_bin, 2)
                              .and_return(MapReturnType::KEY_VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "b" => 2, "d" => 2 })
    end
  end

  describe "MapOperation.get_by_value_list" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3, "d" => 2 } }

    it "gets the items identified by a list of values" do
      operation = MapOperation.get_by_value_list(map_bin, [2, 3])
                              .and_return(MapReturnType::KEY_VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "b" => 2, "c" => 3, "d" => 2 })
    end
  end

  describe "MapOperation.get_by_value_range" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3, "d" => 2 } }

    it "gets the specified key range from the map" do
      operation = MapOperation.get_by_value_range(map_bin, 2, 3)
                              .and_return(MapReturnType::KEY_VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "b" => 2, "d" => 2 })
    end

    it "gets all values from the specified start value until the end" do
      operation = MapOperation.get_by_value_range(map_bin, 2)
                              .and_return(MapReturnType::KEY_VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "b" => 2, "d" => 2, "c" => 3 })
    end

    it "gets all values from the start of the map until the specified end value" do
      operation = MapOperation.get_by_value_range(map_bin, nil, 3)
                              .and_return(MapReturnType::KEY_VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "a" => 1, "b" => 2, "d" => 2 })
    end
  end

  describe "MapOperation.get_by_value_rel_rank_range", skip: !Support.min_version?("4.3") do
    let(:map_value) { { 4 => 2, 9 => 10, 5 => 15, 0 => 17 } }

    it "gets specified number of elements" do
      operation = MapOperation.get_by_value_rel_rank_range(map_bin, 11, -1, 1)
                              .and_return(MapReturnType::KEY_VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ 9 => 10 })
    end

    it "gets elements from specified key until the end" do
      operation = MapOperation.get_by_value_rel_rank_range(map_bin, 11, -1)
                              .and_return(MapReturnType::KEY_VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ 9 => 10, 5 => 15, 0 => 17 })
    end
  end

  describe "MapOperation.get_by_index" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "gets a map item identified by index from the map" do
      operation = MapOperation.get_by_index(map_bin, 1)
                              .and_return(MapReturnType::KEY_VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "b" => 2 })
    end
  end

  describe "MapOperation.get_by_index_range" do
    let(:map_value) { { "c" => 1, "b" => 2, "a" => 3 } }

    it "gets 'count' map items starting at the specified index from the map" do
      operation = MapOperation.get_by_index_range(map_bin, 1, 2)
                              .and_return(MapReturnType::KEY)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql(%w[b c])
    end

    it "gets all items starting at the specified index to the end of the map" do
      operation = MapOperation.get_by_index_range(map_bin, 1)
                              .and_return(MapReturnType::KEY)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql(%w[b c])
    end
  end

  describe "MapOperation.get_by_rank" do
    let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

    it "gets a map item identified by rank from the map" do
      operation = MapOperation.get_by_rank(map_bin, 1)
                              .and_return(MapReturnType::KEY_VALUE)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql({ "b" => 2 })
    end
  end

  describe "MapOperation.get_by_rank_range" do
    let(:map_value) { { "a" => 3, "b" => 2, "c" => 1 } }

    it "gets 'count' map items starting at the specified rank from the map" do
      operation = MapOperation.get_by_rank_range(map_bin, 1, 2)
                              .and_return(MapReturnType::KEY)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql(%w[b a])
    end

    it "gets all items starting at the specified rank to the end of the map" do
      operation = MapOperation.get_by_rank_range(map_bin, 1)
                              .and_return(MapReturnType::KEY)
      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql(%w[b a])
    end
  end

  context "legacy operations" do
    describe "MapOperation.remove_keys" do
      let(:map_value) { { "a" => 1, "b" => 2, "c" => 3 } }

      it "removes a single key from the map" do
        operation = MapOperation.remove_keys(map_bin, "b")
                                .and_return(MapReturnType::VALUE)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to be(2)
        expect(map_post_op).to eql({ "a" => 1, "c" => 3 })
      end

      it "removes multiple keys from the map" do
        operation = MapOperation.remove_keys(map_bin, "b", "c")
                                .and_return(MapReturnType::VALUE)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to contain_exactly(2, 3)
        expect(map_post_op).to eql({ "a" => 1 })
      end
    end

    describe "MapOperation.remove_values" do
      let(:map_value) { { "a" => 1, "b" => 2, "c" => 3, "d" => 2 } }

      it "removes the items identified by a single value" do
        operation = MapOperation.remove_values(map_bin, 2)
                                .and_return(MapReturnType::KEY)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to contain_exactly("b", "d")
        expect(map_post_op).to eql({ "a" => 1, "c" => 3 })
      end

      it "removes the items identified by multiple values" do
        operation = MapOperation.remove_values(map_bin, 2, 3)
                                .and_return(MapReturnType::KEY)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to contain_exactly("b", "c", "d")
        expect(map_post_op).to eql({ "a" => 1 })
      end
    end
  end

  context "MapReturnType" do
    let(:map_value) { { "a" => 3, "b" => 2, "c" => 1 } }

    context "NONE" do
      it "returns nothing" do
        operation = MapOperation.get_by_key(map_bin, "a")
                                .and_return(MapReturnType::NONE)
        result = client.operate(key, [operation])

        expected = { "map_bin" => nil }
        expect(result.bins).to eq expected
      end
    end

    context "INDEX" do
      it "returns returns the elements index" do
        operation = MapOperation.get_by_key(map_bin, "a")
                                .and_return(MapReturnType::INDEX)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to eql(0)
      end
    end

    context "REVERSE_INDEX" do
      it "returns the elements reverse index" do
        operation = MapOperation.get_by_key(map_bin, "a")
                                .and_return(MapReturnType::REVERSE_INDEX)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to eql(2)
      end
    end

    context "RANK" do
      it "returns the elements rank" do
        operation = MapOperation.get_by_key(map_bin, "a")
                                .and_return(MapReturnType::RANK)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to eql(2)
      end
    end

    context "REVERSE_RANK" do
      it "returns the elements reverse rank" do
        operation = MapOperation.get_by_key(map_bin, "a")
                                .and_return(MapReturnType::REVERSE_RANK)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to eql(0)
      end
    end

    context "COUNT" do
      it "returns the count of items selected" do
        operation = MapOperation.get_by_key_range(map_bin, "a", "c")
                                .and_return(MapReturnType::COUNT)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to eql(2)
      end
    end

    context "KEY" do
      it "returns the map key" do
        operation = MapOperation.get_by_index(map_bin, 0)
                                .and_return(MapReturnType::KEY)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to eql("a")
      end
    end

    context "VALUE" do
      it "returns the map value" do
        operation = MapOperation.get_by_index(map_bin, 0)
                                .and_return(MapReturnType::VALUE)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to eql(3)
      end
    end

    context "KEY_VALUE" do
      it "returns the map key & value" do
        operation = MapOperation.get_by_index(map_bin, 0)
                                .and_return(MapReturnType::KEY_VALUE)
        result = client.operate(key, [operation])

        expect(result.bins[map_bin]).to eql({ "a" => 3 })
      end
    end
  end

  context "MapWriteMode" do
    let(:write_mode) { MapWriteMode::UPDATE }
    let(:map_policy) { MapPolicy.new(write_mode: write_mode) }
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

  context "MapWriteFlags", skip: !Support.min_version?("4.3") do
    let(:write_flags) { MapWriteFlags::DEFAULT }
    let(:map_policy) { MapPolicy.new(flags: write_flags) }
    let(:operation) { MapOperation.put(map_bin, "b", 99, policy: map_policy) }

    context "DEFAULT" do
      let(:write_flags) { MapWriteFlags::DEFAULT }

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

    context "CREATE_ONLY" do
      let(:write_flags) { MapWriteFlags::CREATE_ONLY }

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

          context "NO_FAIL" do
            let(:write_flags) { MapWriteFlags::CREATE_ONLY | MapWriteFlags::NO_FAIL }

            it "succeeds, but does not update the element" do
              client.operate(key, [operation])

              expect(map_post_op).to eql({ "a" => 1, "b" => 2, "c" => 3 })
            end

            context "PARTIAL" do
              let(:write_flags) { MapWriteFlags::CREATE_ONLY | MapWriteFlags::NO_FAIL | MapWriteFlags::PARTIAL }
              let(:operation) { MapOperation.put_items(map_bin, { "b" => 99, "d" => 4 }, policy: map_policy) }

              it "inserts the unique items" do
                client.operate(key, [operation])

                expect(map_post_op).to eql({ "a" => 1, "b" => 2, "c" => 3, "d" => 4 })
              end
            end
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

    context "UPDATE_ONLY" do
      let(:write_flags) { MapWriteFlags::UPDATE_ONLY }

      context "map does not exist" do
        it "returns an error" do
          expect { client.operate(key, [operation]) }.to raise_error(/Element not found/)
        end

        context "NO_FAIL" do
          let(:write_flags) { MapWriteFlags::UPDATE_ONLY | MapWriteFlags::NO_FAIL }

          it "succeeds, but does not insert the element" do
            client.operate(key, [operation])

            expect(map_post_op).to eql({})
          end
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

          context "NO_FAIL" do
            let(:write_flags) { MapWriteFlags::UPDATE_ONLY | MapWriteFlags::NO_FAIL }

            it "does not insert the element" do
              client.operate(key, [operation])

              expect(map_post_op).to eql({ "a" => 1, "c" => 3 })
            end

            context "PARTIAL" do
              let(:write_flags) { MapWriteFlags::UPDATE_ONLY | MapWriteFlags::NO_FAIL | MapWriteFlags::PARTIAL }
              let(:operation) { MapOperation.put_items(map_bin, { "b" => 99, "c" => 100 }, policy: map_policy) }

              it "updates the existing elements" do
                client.operate(key, [operation])

                expect(map_post_op).to eql({ "a" => 1, "c" => 100 })
              end
            end
          end
        end
      end
    end
  end

  context "Infinity value", skip: !Support.min_version?("4.3.1") do
    let(:map_value) { { 0 => 17, 4 => 2, 5 => 15, 9 => 10 } }

    it "returns all keys from 5 to Infinity" do
      operation = MapOperation.get_by_key_range(map_bin, 5, Aerospike::Value::INFINITY)
                              .and_return(MapReturnType::KEY)

      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql([5, 9])
    end
  end

  context "Wildcard value", skip: !Support.min_version?("4.3.1") do
    let(:map_value) do
      {
        4 => ["John", 55],
        5 => ["Jim", 95],
        9 => ["Joe", 80],
        12 => ["Jim", 46]
      }
    end

    it "returns all values that match a wildcard" do
      operation = MapOperation.get_by_value(map_bin, ["Jim", Aerospike::Value::WILDCARD])
                              .and_return(MapReturnType::KEY)

      result = client.operate(key, [operation])

      expect(result.bins[map_bin]).to eql([5, 12])
    end
  end
end
