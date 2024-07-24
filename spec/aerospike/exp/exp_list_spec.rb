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

describe Aerospike::Exp::List do
  let(:client) { Support.client }

  describe "Expression filters" do
    before :all do
      @key_count = 100
      @namespace = "test"
      @set = "query1000"

      opts = { }
      @key_count.times do |ii|
        key = Aerospike::Key.new(@namespace, @set, ii)
        ibin = { "bin" => [1, 2, 3, ii] }
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

    it "append should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.size(
            Exp::List.append(
              Exp.int_val(999),
              Exp::list_bin("bin"),
            ),
          ),
          Exp.int_val(5),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "append_items should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.size(
            Exp::List.append_items(
              Exp::list_val(Aerospike::Value.of(555), Aerospike::Value.of("asd")),
              Exp::list_bin("bin"),
            ),
          ),
          Exp.int_val(6),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "clear should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.size(
            Exp::List.clear(Exp::list_bin("bin")),
          ),
          Exp.int_val(0),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "ListReturnType::Count should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.get_by_value(
            Aerospike::CDT::ListReturnType::COUNT,
            Exp.int_val(234),
            Exp::List.insert(
              Exp.int_val(1),
              Exp.int_val(234),
              Exp::list_bin("bin"),
            ),
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "ListReturnType::Count should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.get_by_value_list(
            Aerospike::CDT::ListReturnType::COUNT,
            Exp::list_val(Aerospike::Value.of(51), Aerospike::Value.of(52)),
            Exp::list_bin("bin"),
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 2
    end

    it "insert_items should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.size(
            Exp::List.insert_items(
              Exp.int_val(4),
              Exp::list_val(Aerospike::Value.of(222), Aerospike::Value.of(223)),
              Exp::list_bin("bin"),
            ),
          ),
          Exp.int_val(6),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "ListReturnType::Value should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.get_by_index(
            Aerospike::CDT::ListReturnType::VALUE,
            Exp::Type::INT,
            Exp.int_val(3),
            Exp::List.increment(
              Exp.int_val(3),
              Exp.int_val(100),
              Exp::list_bin("bin"),
            ),
          ),
          Exp.int_val(102),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 1
    end

    it "ListReturnType::Value should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.get_by_index(
            Aerospike::CDT::ListReturnType::VALUE,
            Exp::Type::INT,
            Exp.int_val(3),
            Exp::List.set(
              Exp.int_val(3),
              Exp.int_val(100),
              Exp::list_bin("bin"),
            ),
          ),
          Exp.int_val(100),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "ListReturnType::Value should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.get_by_index_range(
            Aerospike::CDT::ListReturnType::VALUE,
            Exp.int_val(2),
            Exp::list_bin("bin"),
            count: 2,
          ),
          Exp::list_val(Aerospike::Value.of(3), Aerospike::Value.of(15)),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 1
    end

    it "ListReturnType::Value should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.get_by_index_range(
            Aerospike::CDT::ListReturnType::VALUE,
            Exp.int_val(2),
            Exp::list_bin("bin"),
          ),
          Exp::list_val(Aerospike::Value.of(3), Aerospike::Value.of(15)),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 1
    end

    it "ListReturnType::Value should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.get_by_rank(
            Aerospike::CDT::ListReturnType::VALUE,
            Exp::Type::INT,
            Exp.int_val(3),
            Exp::list_bin("bin"),
          ),
          Exp.int_val(25),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 1
    end

    it "ListReturnType::Value should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.get_by_rank_range(
            Aerospike::CDT::ListReturnType::VALUE,
            Exp.int_val(2),
            Exp::list_bin("bin"),
          ),
          Exp::list_val(Aerospike::Value.of(3), Aerospike::Value.of(25)),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 1
    end

    it "ListReturnType::Value should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.get_by_rank_range(
            Aerospike::CDT::ListReturnType::VALUE,
            Exp.int_val(2),
            Exp::list_bin("bin"),
            count: 2,
          ),
          Exp::list_val(Aerospike::Value.of(3), Aerospike::Value.of(3)),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 1
    end

    it "ListReturnType::Value should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.get_by_value_range(
            Aerospike::CDT::ListReturnType::VALUE,
            Exp.int_val(1),
            Exp.int_val(3),
            Exp::list_bin("bin"),
          ),
          Exp::list_val(Aerospike::Value.of(1), Aerospike::Value.of(2)),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 98
    end

    it "ListReturnType::Count should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.get_by_value_relative_rank_range(
            Aerospike::CDT::ListReturnType::COUNT,
            Exp.int_val(2),
            Exp.int_val(0),
            Exp::list_bin("bin"),
          ),
          Exp.int_val(3),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 98
    end

    it "ListReturnType::Value should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.get_by_value_relative_rank_range(
            Aerospike::CDT::ListReturnType::VALUE,
            Exp.int_val(2),
            Exp.int_val(1),
            Exp::list_bin("bin"),
            count: 1,
          ),
          Exp::list_val(Aerospike::Value.of(3)),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 99
    end

    it "remove_by_value should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.size(
            Exp::List.remove_by_value(
              Exp.int_val(3),
              Exp::list_bin("bin"),
            ),
          ),
          Exp.int_val(3),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 99
    end

    it "remove_by_value_list should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.size(
            Exp::List.remove_by_value_list(
              Exp::list_val(Aerospike::Value.of(1), Aerospike::Value.of(2)),
              Exp::list_bin("bin"),
            ),
          ),
          Exp.int_val(2),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 98
    end

    it "remove_by_value_range should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.size(
            Exp::List.remove_by_value_range(
              Exp.int_val(1),
              Exp.int_val(3),
              Exp::list_bin("bin"),
            ),
          ),
          Exp.int_val(2),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 98
    end

    it "remove_by_value_relative_rank_range should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.size(
            Exp::List.remove_by_value_relative_rank_range(
              Exp.int_val(3),
              Exp.int_val(1),
              Exp::list_bin("bin"),
            ),
          ),
          Exp.int_val(3),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 97
    end

    it "remove_by_value_relative_rank_range for count should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.size(
            Exp::List.remove_by_value_relative_rank_range(
              Exp.int_val(2),
              Exp.int_val(1),
              Exp::list_bin("bin"),
              count: 1,
            ),
          ),
          Exp.int_val(3),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove_by_index should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.size(
            Exp::List.remove_by_index(
              Exp.int_val(0),
              Exp::list_bin("bin"),
            ),
          ),
          Exp.int_val(3),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove_by_index_range should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.size(
            Exp::List.remove_by_index_range(
              Exp.int_val(2),
              Exp::list_bin("bin"),
            ),
          ),
          Exp.int_val(2),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove_by_index_range for count should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.size(
            Exp::List.remove_by_index_range(
              Exp.int_val(2),
              Exp::list_bin("bin"),
              count: 1,
            ),
          ),
          Exp.int_val(3),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove_by_index_range for count should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.size(
            Exp::List.remove_by_index_range(
              Exp.int_val(2),
              Exp::list_bin("bin"),
              count: 1,
            ),
          ),
          Exp.int_val(3),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove_by_rank should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.size(
            Exp::List.remove_by_rank(
              Exp.int_val(2),
              Exp::list_bin("bin"),
            ),
          ),
          Exp.int_val(3),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove_by_rank_range should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.size(
            Exp::List.remove_by_rank_range(
              Exp.int_val(2),
              Exp::list_bin("bin"),
            ),
          ),
          Exp.int_val(2),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove_by_rank_range for count should work" do
      rs = run_query(
        Exp.eq(
          Exp::List.size(
            Exp::List.remove_by_rank_range(
              Exp.int_val(2),
              Exp::list_bin("bin"),
              count: 1,
            ),
          ),
          Exp.int_val(3),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end
  end
end
