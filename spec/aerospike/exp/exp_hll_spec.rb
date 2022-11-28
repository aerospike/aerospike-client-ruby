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

describe Aerospike::Exp::HLL do
  let(:client) { Support.client }

  describe "Expression filters" do
    before :all do
      @key_count = 100
      @namespace = "test"
      @set = "query1000"

      Support.client.truncate(@namespace, @set)

      opts = { expiration: 24 * 60 * 60 }
      @key_count.times do |ii|
        key = Aerospike::Key.new(@namespace, @set, ii)
        bin = { "bin" => ii, "lbin" => [ii, "a"] }
        Support.client.delete(key)
        Support.client.put(key, bin)

        data = ["asd", ii]
        data2 = ["asd", ii, ii + 1]

        ops = [
          Aerospike::CDT::HLLOperation.add("hllbin", *data, index_bit_count: 8, minhash_bit_count: 0),
          Aerospike::CDT::HLLOperation.add("hllbin2", *data2, index_bit_count: 8, minhash_bit_count: 0),
        ]

        Support.client.operate(key, ops)
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

    it "get_count should work" do
      rs = run_query(
        Exp.eq(
          Exp::HLL.get_count(
            Exp::HLL.add(
              Exp.list_val(Aerospike::Value.of(48715414)),
              Exp.hll_bin("hllbin"),
              index_bit_count: Exp.int_val(8),
              min_hash_bit_count: Exp.int_val(0),
            ),
          ),
          Exp.int_val(3),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 99
    end

    it "may_contain should work" do
      rs = run_query(
        Exp.eq(
          Exp::HLL.may_contain(
            Exp.list_val(Aerospike::Value.of(55)),
            Exp.hll_bin("hllbin"),
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 1
    end

    it "list_get_by_index should work" do
      rs = run_query(
        Exp.lt(
          Exp::List.get_by_index(
            Aerospike::CDT::ListReturnType::VALUE,
            Exp::Type::INT,
            Exp.int_val(0),
            Exp::HLL.describe(Exp.hll_bin("hllbin")),
          ),
          Exp.int_val(10),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "get_union should work" do
      rs = run_query(
        Exp.eq(
          Exp::HLL.get_count(
            Exp::HLL.get_union(
              Exp.hll_bin("hllbin"),
              Exp.hll_bin("hllbin2"),
            ),
          ),
          Exp.int_val(3),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 98
    end

    it "get_union_count should work" do
      rs = run_query(
        Exp.eq(
          Exp::HLL.get_union_count(
            Exp.hll_bin("hllbin"),
            Exp.hll_bin("hllbin2"),
          ),
          Exp.int_val(3),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 98
    end

    it "get_intersect_count should work" do
      rs = run_query(
        Exp.eq(
          Exp::HLL.get_intersect_count(
            Exp.hll_bin("hllbin"),
            Exp.hll_bin("hllbin2"),
          ),
          Exp.int_val(2),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 99
    end

    it "get_similarity should work" do
      rs = run_query(
        Exp.gt(
          Exp::HLL.get_similarity(
            Exp.hll_bin("hllbin"),
            Exp.hll_bin("hllbin2"),
          ),
          Exp.float_val(0.5),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 99
    end
  end
end
