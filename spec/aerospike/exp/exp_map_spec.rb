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

require "aerospike/query/statement"
include Aerospike

describe Aerospike::Exp::Map do
  let(:client) { Support.client }

  describe "Expression filters" do
    before :all do
      @key_count = 100
      @namespace = "test"
      @set = "query1000"

      opts = { expiration: 24 * 60 * 60 }
      @key_count.times do |ii|
        key = Aerospike::Key.new(@namespace, @set, ii)
        ibin = { "bin" => { "test" => ii, "test2" => "a" } }
        Support.client.delete(key, opts)
        Support.client.put(key, ibin, opts)
      end
    end

    def run_query(filter)
      opts = { filter_exp: filter }
      stmt = Aerospike::Statement.new(@namespace, @set)
      client.query(stmt, opts)
    end

    def count_results(rs)
      count = 0
      rs.each do
        count += 1
      end
      count
    end

    it "get_by_key should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.get_by_key(
            Aerospike::CDT::MapReturnType::VALUE,
            Exp::Type::INT,
            Exp.str_val("test3"),
            Exp::Map.put(
              Exp.str_val("test3"),
              Exp.int_val(999),
              Exp::map_bin("bin"),
            ),
          ),
          Exp.int_val(999),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "get_by_key_list should work" do
      amap = {
        "test4" => 333,
        "test5" => 444,
      }
      rs = run_query(
        Exp.eq(
          Exp::Map.get_by_key_list(
            Aerospike::CDT::MapReturnType::VALUE,
            Exp.list_val(Aerospike::Value.of("test4"), Aerospike::Value.of("test5")),
            Exp::Map.put_items(
              Exp::map_val(amap),
              Exp::map_bin("bin"),
            ),
          ),
          Exp.list_val(Aerospike::Value.of(333), Aerospike::Value.of(444)),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "get_by_value should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.get_by_value(
            Aerospike::CDT::MapReturnType::COUNT,
            Exp.int_val(5),
            Exp::Map.increment(
              Exp.str_val("test"),
              Exp.int_val(1),
              Exp::map_bin("bin"),
            ),
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 1
    end

    it "Clear should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.size(
            Exp::Map.clear(Exp::map_bin("bin")),
          ),
          Exp.int_val(0),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "get_by_value_list should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.get_by_value_list(
            Aerospike::CDT::MapReturnType::COUNT,
            Exp.list_val(Aerospike::Value.of(1), Aerospike::Value.of("a")),
            Exp::map_bin("bin"),
          ),
          Exp.int_val(2),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 1
    end

    it "get_by_value_relative_rank_range should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.get_by_value_relative_rank_range(
            Aerospike::CDT::MapReturnType::COUNT,
            Exp.int_val(1),
            Exp.int_val(0),
            Exp::map_bin("bin"),
          ),
          Exp.int_val(2),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 99
    end

    it "get_by_value_relative_rank_range with count should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.get_by_value_relative_rank_range(
            Aerospike::CDT::MapReturnType::COUNT,
            Exp.int_val(1),
            Exp.int_val(0),
            Exp::map_bin("bin"),
            count: 1,
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "get_by_index should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.get_by_index(
            Aerospike::CDT::MapReturnType::VALUE,
            Exp::Type::INT,
            Exp.int_val(0),
            Exp::map_bin("bin"),
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 1
    end

    it "get_by_index_range should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.get_by_index_range(
            Aerospike::CDT::MapReturnType::COUNT,
            Exp.int_val(0),
            Exp::map_bin("bin"),
          ),
          Exp.int_val(2),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "get_by_index_range with count should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.get_by_index_range(
            Aerospike::CDT::MapReturnType::VALUE,
            Exp.int_val(0),
            Exp::map_bin("bin"),
            count: 1,
          ),
          Exp.list_val(Aerospike::Value.of(2)),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 1
    end

    it "get_by_rank should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.get_by_rank(
            Aerospike::CDT::MapReturnType::VALUE,
            Exp::Type::INT,
            Exp.int_val(0),
            Exp::map_bin("bin"),
          ),
          Exp.int_val(2),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 1
    end

    it "get_by_rank_range should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.get_by_rank_range(
            Aerospike::CDT::MapReturnType::VALUE,
            Exp.int_val(1),
            Exp::map_bin("bin"),
          ),
          Exp.list_val(Aerospike::Value.of("a")),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "get_by_rank_range with count should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.get_by_rank_range(
            Aerospike::CDT::MapReturnType::VALUE,
            Exp.int_val(0),
            Exp::map_bin("bin"),
            count: 1,
          ),
          Exp.list_val(Aerospike::Value.of(15)),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 1
    end

    it "get_by_value_range should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.get_by_value_range(
            Aerospike::CDT::MapReturnType::COUNT,
            Exp.int_val(0),
            Exp.int_val(18),
            Exp::map_bin("bin"),
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 18
    end

    it "get_by_key_range should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.get_by_key_range(
            Aerospike::CDT::MapReturnType::COUNT,
            nil,
            Exp.str_val("test25"),
            Exp::map_bin("bin"),
          ),
          Exp.int_val(2),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "get_by_key_relative_index_range should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.get_by_key_relative_index_range(
            Aerospike::CDT::MapReturnType::COUNT,
            Exp.str_val("test"),
            Exp.int_val(0),
            Exp::map_bin("bin"),
          ),
          Exp.int_val(2),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "get_by_key_relative_index_range with count should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.get_by_key_relative_index_range(
            Aerospike::CDT::MapReturnType::COUNT,
            Exp.str_val("test"),
            Exp.int_val(0),
            Exp::map_bin("bin"),
            count: 1,
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove_by_key should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.size(
            Exp::Map.remove_by_key(
              Exp.str_val("test"),
              Exp::map_bin("bin"),
            ),
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove_by_key_list should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.size(
            Exp::Map.remove_by_key_list(
              Exp.list_val(Aerospike::Value.of("test"), Aerospike::Value.of("test2")),
              Exp::map_bin("bin"),
            ),
          ),
          Exp.int_val(0),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove_by_key_range should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.size(
            Exp::Map.remove_by_key_range(
              Exp.str_val("test"),
              nil,
              Exp::map_bin("bin"),
            ),
          ),
          Exp.int_val(0),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove_by_key_relative_index_range should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.size(
            Exp::Map.remove_by_key_relative_index_range(
              Exp.str_val("test"),
              Exp.int_val(0),
              Exp::map_bin("bin"),
            ),
          ),
          Exp.int_val(0),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove_by_key_relative_index_range with count should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.size(
            Exp::Map.remove_by_key_relative_index_range(
              Exp.str_val("test"),
              Exp.int_val(0),
              Exp::map_bin("bin"),
              count: 1,
            ),
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove_by_value should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.size(
            Exp::Map.remove_by_value(
              Exp.int_val(5),
              Exp::map_bin("bin"),
            ),
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 1
    end

    it "remove_by_value_list should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.size(
            Exp::Map.remove_by_value_list(
              Exp.list_val(Aerospike::Value.of("a"), Aerospike::Value.of(15)),
              Exp::map_bin("bin"),
            ),
          ),
          Exp.int_val(0),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 1
    end

    it "remove_by_value_range should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.size(
            Exp::Map.remove_by_value_range(
              Exp.int_val(5),
              Exp.int_val(15),
              Exp::map_bin("bin"),
            ),
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 10
    end

    it "remove_by_index should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.size(
            Exp::Map.remove_by_index(
              Exp.int_val(0),
              Exp::map_bin("bin"),
            ),
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove_by_index_range should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.size(
            Exp::Map.remove_by_index_range(
              Exp.int_val(0),
              Exp::map_bin("bin"),
            ),
          ),
          Exp.int_val(0),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove_by_index_range with count should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.size(
            Exp::Map.remove_by_index_range(
              Exp.int_val(0),
              Exp::map_bin("bin"),
              count: 1,
            ),
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove_by_rank should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.size(
            Exp::Map.remove_by_rank(
              Exp.int_val(0),
              Exp::map_bin("bin"),
            ),
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove_by_rank_range should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.size(
            Exp::Map.remove_by_rank_range(
              Exp.int_val(0),
              Exp::map_bin("bin"),
            ),
          ),
          Exp.int_val(0),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove_by_rank_range with count should work" do
      rs = run_query(
        Exp.eq(
          Exp::Map.size(
            Exp::Map.remove_by_rank_range(
              Exp.int_val(0),
              Exp::map_bin("bin"),
              count: 1,
            ),
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end
  end
end
