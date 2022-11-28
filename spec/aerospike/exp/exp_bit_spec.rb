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

describe Aerospike::Exp::Bit do
  let(:client) { Support.client }

  describe "Expression filters", skip: false do
    before :all do
      @key_count = 100
      @namespace = "test"
      @set = "query1000"
      Support.client.truncate(@namespace, @set)

      opts = { expiration: 24 * 60 * 60 }
      @key_count.times do |ii|
        key = Aerospike::Key.new(@namespace, @set, ii)
        bytes = bytes_to_str([0b00000001, 0b01000010])
        bin = { "bin" => Aerospike::BytesValue.new(bytes) }
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

    def bytes_to_str(bytes)
      bytes.pack("C*").force_encoding("binary")
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

    it "count should work" do
      rs = run_query(
        Exp.eq(
          Exp::Bit.count(
            Exp.int_val(0),
            Exp.int_val(16),
            Exp.blob_bin("bin"),
          ),
          Exp.int_val(3),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "resize should work" do
      rs = run_query(
        Exp.eq(
          Exp::Bit.count(
            Exp.int_val(0),
            Exp.int_val(16),
            Exp::Bit.resize(
              Exp.int_val(4),
              Aerospike::CDT::BitResizeFlags::DEFAULT,
              Exp.blob_bin("bin"),
            ),
          ),
          Exp.int_val(3),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "insert should work" do
      rs = run_query(
        Exp.eq(
          Exp::Bit.count(
            Exp.int_val(0),
            Exp.int_val(16),
            Exp::Bit.insert(
              Exp.int_val(0),
              Exp.blob_val(bytes_to_str([0b11111111])),
              Exp.blob_bin("bin"),
            ),
          ),
          Exp.int_val(9),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "remove should work" do
      rs = run_query(
        Exp.eq(
          Exp::Bit.count(
            Exp.int_val(0),
            Exp.int_val(8),
            Exp::Bit.remove(
              Exp.int_val(0),
              Exp.int_val(1),
              Exp.blob_bin("bin"),
            ),
          ),
          Exp.int_val(2),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "set should work" do
      rs = run_query(
        Exp.eq(
          Exp::Bit.count(
            Exp.int_val(0),
            Exp.int_val(8),
            Exp::Bit.set(
              Exp.int_val(0),
              Exp.int_val(8),
              Exp.blob_val(bytes_to_str([0b10101010])),
              Exp.blob_bin("bin"),
            ),
          ),
          Exp.int_val(4),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "or should work" do
      rs = run_query(
        Exp.eq(
          Exp::Bit.count(
            Exp.int_val(0),
            Exp.int_val(8),
            Exp::Bit.or(
              Exp.int_val(0),
              Exp.int_val(8),
              Exp.blob_val(bytes_to_str([0b10101010])),
              Exp.blob_bin("bin"),
            ),
          ),
          Exp.int_val(5),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "xor should work" do
      rs = run_query(
        Exp.eq(
          Exp::Bit.count(
            Exp.int_val(0),
            Exp.int_val(8),
            Exp::Bit.xor(
              Exp.int_val(0),
              Exp.int_val(8),
              Exp.blob_val(bytes_to_str([0b10101011])),
              Exp.blob_bin("bin"),
            ),
          ),
          Exp.int_val(4),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "and should work" do
      rs = run_query(
        Exp.eq(
          Exp::Bit.count(
            Exp.int_val(0),
            Exp.int_val(8),
            Exp::Bit.and(
              Exp.int_val(0),
              Exp.int_val(8),
              Exp.blob_val(bytes_to_str([0b10101011])),
              Exp.blob_bin("bin"),
            ),
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "not should work" do
      rs = run_query(
        Exp.eq(
          Exp::Bit.count(
            Exp.int_val(0),
            Exp.int_val(8),
            Exp::Bit.not(
              Exp.int_val(0),
              Exp.int_val(8),
              Exp.blob_bin("bin"),
            ),
          ),
          Exp.int_val(7),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "lshift should work" do
      rs = run_query(
        Exp.eq(
          Exp::Bit.count(
            Exp.int_val(0),
            Exp.int_val(8),
            Exp::Bit.lshift(
              Exp.int_val(0),
              Exp.int_val(16),
              Exp.int_val(9),
              Exp.blob_bin("bin"),
            ),
          ),
          Exp.int_val(2),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "rshift should work" do
      rs = run_query(
        Exp.eq(
          Exp::Bit.count(
            Exp.int_val(0),
            Exp.int_val(8),
            Exp::Bit.rshift(
              Exp.int_val(0),
              Exp.int_val(8),
              Exp.int_val(3),
              Exp.blob_bin("bin"),
            ),
          ),
          Exp.int_val(0),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "add should work" do
      rs = run_query(
        Exp.eq(
          Exp::Bit.count(
            Exp.int_val(0),
            Exp.int_val(8),
            Exp::Bit.add(
              Exp.int_val(0),
              Exp.int_val(8),
              Exp.int_val(128),
              false,
              Aerospike::CDT::BitOverflowAction::WRAP,
              Exp.blob_bin("bin"),
            ),
          ),
          Exp.int_val(2),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "subtract should work" do
      rs = run_query(
        Exp.eq(
          Exp::Bit.count(
            Exp.int_val(0),
            Exp.int_val(8),
            Exp::Bit.subtract(
              Exp.int_val(0),
              Exp.int_val(8),
              Exp.int_val(1),
              false,
              Aerospike::CDT::BitOverflowAction::WRAP,
              Exp.blob_bin("bin"),
            ),
          ),
          Exp.int_val(0),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it " set_int should work" do
      rs = run_query(
        Exp.eq(
          Exp::Bit.count(
            Exp.int_val(0),
            Exp.int_val(8),
            Exp::Bit.set_int(
              Exp.int_val(0),
              Exp.int_val(8),
              Exp.int_val(255),
              Exp.blob_bin("bin"),
            ),
          ),
          Exp.int_val(8),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "get should work" do
      rs = run_query(
        Exp.eq(
          Exp::Bit.get(
            Exp.int_val(0),
            Exp.int_val(8),
            Exp.blob_bin("bin"),
          ),
          Exp.blob_val(bytes_to_str([0b00000001])),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "lscan should work" do
      rs = run_query(
        Exp.eq(
          Exp::Bit.lscan(
            Exp.int_val(8),
            Exp.int_val(8),
            Exp.bool_val(true),
            Exp.blob_bin("bin"),
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "rscan should work" do
      rs = run_query(
        Exp.eq(
          Exp::Bit.rscan(
            Exp.int_val(8),
            Exp.int_val(8),
            Exp.bool_val(true),
            Exp.blob_bin("bin"),
          ),
          Exp.int_val(6),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end

    it "get_int should work" do
      rs = run_query(
        Exp.eq(
          Exp::Bit.get_int(
            Exp.int_val(0),
            Exp.int_val(8),
            false,
            Exp.blob_bin("bin"),
          ),
          Exp.int_val(1),
        ),
      )
      count = count_results(rs)
      expect(count).to eq 100
    end
  end
end
