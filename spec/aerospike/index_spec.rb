# encoding: utf-8

# Copyright 2014-2020 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require "aerospike/host"
require "aerospike/key"
require "aerospike/bin"
require "aerospike/language"

describe Aerospike::Client do
  let(:client) { Support.client }

  let(:str_bin_name) { "str_bin" }
  let(:int_bin_name) { "int_bin" }
  let(:list_bin_name) { "list_bin" }
  let(:list_bin_ctx_name) { "list_bin_ctx" }
  let(:map_bin_name) { "map_bin" }

  before do
    key = Support.gen_random_key
    client.truncate(key.namespace, key.set_name)
    (1..1000).to_a.each do |i|
      key = Support.gen_random_key
      record = {
        int_bin_name => rand(100_000),
        str_bin_name => "string value",
        list_bin_name => Array.new(10) { rand(1000) },
        list_bin_ctx_name => Array.new(5) { |rank| i + rank },
        map_bin_name => { i: rand(100_000), s: "map string value" },
      }
      client.put(key, record)
    end
  end

  after do
    # clean up indexes
    key = Support.gen_random_key
    client.drop_index(key.namespace,
                      key.set_name,
                      "index_str_#{key.set_name}")
    client.drop_index(key.namespace,
                      key.set_name,
                      "index_int_#{key.set_name}")
    client.drop_index(key.namespace,
                      key.set_name,
                      "index_list_#{key.set_name}")
    client.drop_index(key.namespace,
                      key.set_name,
                      "index_mapkeys_#{key.set_name}")
    client.drop_index(key.namespace,
                      key.set_name,
                      "index_mapvalues_#{key.set_name}")
    client.drop_index(key.namespace,
                      key.set_name,
                      "index_list_values_context_#{key.set_name}")
  end

  describe "Index operations" do
    it "should create an integer index and wait until it is created on all nodes" do
      key = Support.gen_random_key
      index_task = client.create_index(key.namespace,
                                       key.set_name,
                                       "index_int_#{key.set_name}",
                                       int_bin_name, :numeric)

      expect(index_task.wait_till_completed).to be true
      expect(index_task.completed?).to be true
    end

    it "should create a string index and wait until it is created on all nodes" do
      key = Support.gen_random_key
      index_task = client.create_index(key.namespace,
                                       key.set_name,
                                       "index_str_#{key.set_name}",
                                       str_bin_name, :string)

      expect(index_task.wait_till_completed).to be true
      expect(index_task.completed?).to be true
    end

    it "should create an index on a list and wait until it is created on all nodes" do
      key = Support.gen_random_key
      index_task = client.create_index(key.namespace,
                                       key.set_name,
                                       "index_list_#{key.set_name}",
                                       list_bin_name, :numeric, :list)

      expect(index_task.wait_till_completed).to be true
      expect(index_task.completed?).to be true
    end

    it "should create an index on a map keys and wait until it is created on all nodes" do
      key = Support.gen_random_key
      index_task = client.create_index(key.namespace,
                                       key.set_name,
                                       "index_mapkeys_#{key.set_name}",
                                       map_bin_name, :string, :mapkeys)

      expect(index_task.wait_till_completed).to be true
      expect(index_task.completed?).to be true
    end

    it "should create an index on a map values and wait until it is created on all nodes" do
      key = Support.gen_random_key
      index_task = client.create_index(key.namespace,
                                       key.set_name,
                                       "index_mapvalues_#{key.set_name}",
                                       map_bin_name, :string, :mapvalues)

      expect(index_task.wait_till_completed).to be true
      expect(index_task.completed?).to be true
    end

    it "does not return an error when creating an index that already exists" do
      key = Support.gen_random_key
      index_task = client.create_index(key.namespace, key.set_name, "index_int_#{key.set_name}", int_bin_name, :numeric)
      expect(index_task.wait_till_completed).to be true
      expect(index_task.completed?).to be true

      index_task = client.create_index(key.namespace, key.set_name, "index_int_#{key.set_name}", int_bin_name, :numeric)
      expect(index_task.wait_till_completed).to be true
      expect(index_task.completed?).to be true
    end

    it "should create an index on list bin via context" do
      key = Support.gen_random_key
      context = [Aerospike::CDT::Context.list_rank(-1)]
      index_task = client.create_index(key.namespace, key.set_name, "index_list_values_context_#{key.set_name}", list_bin_ctx_name, :numeric, ctx: context)
      expect(index_task.wait_till_completed).to be true
      expect(index_task.completed?).to be true

      start = 14
      finish = 18
      stmt = Aerospike::Statement.new(key.namespace, key.set_name)
      stmt.filters = [Aerospike::Filter.Range(list_bin_ctx_name, start, finish, ctx: context)]
      rs = client.query(stmt)
      count = 0
      rs.each do |r|
        expect(r.bins[list_bin_ctx_name][-1]).to be_between(start, finish)
        count += 1
      end
      expect(count).to eq(5)
    end
  end # describe
end # describe
