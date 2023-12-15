# encoding: utf-8
# Copyright 2014-2023 Aerospike, Inc.
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

describe Aerospike::Exp do
  let(:client) { Support.client }

  describe "Expression filters" do
    before :all do
      @namespace = "test"
      @set = "query1000"
      @record_count = 1000
      @record_count.times do |i|
        key = Aerospike::Key.new(@namespace, @set, i)
        bin_map = {
          "bin1" => "value#{i}",
          "bin2" => i,
          "bin3" => [i, i + 1_000, i + 1_000_000],
          "bin4" => { "key#{i}" => i },
        }
        Support.client.put(key, bin_map)
      end

      Support.client.drop_index(@namespace, @set, "index_intval")
      Support.client.drop_index(@namespace, @set, "index_strval")

      wpolicy = { generation: 0, expiration: 24 * 60 * 60 }

      starbucks = [
        [-122.1708441, 37.4241193],
        [-122.1492040, 37.4273569],
        [-122.1441078, 37.4268202],
        [-122.1251714, 37.4130590],
        [-122.0964289, 37.4218102],
        [-122.0776641, 37.4158199],
        [-122.0943475, 37.4114654],
        [-122.1122861, 37.4028493],
        [-122.0947230, 37.3909250],
        [-122.0831037, 37.3876090],
        [-122.0707119, 37.3787855],
        [-122.0303178, 37.3882739],
        [-122.0464861, 37.3786236],
        [-122.0582128, 37.3726980],
        [-122.0365083, 37.3676930],
      ]

      @record_count.times do |ii|

        # On iteration 333 we pause for a few mSec and note the
        # time.  Later we can check last_update time for either
        # side of this gap ...
        #
        # Also, we update the WritePolicy to never expire so
        # records w/ 0 TTL can be counted later.

        key = Aerospike::Key.new(@namespace, @set, ii)

        lng = -122.0 + (0.01 * ii)
        lat = 37.5 + (0.01 * ii)
        point = Aerospike::GeoJSON.point(lat, lng)

        if ii < starbucks.length
          region = Aerospike::GeoJSON.circle(starbucks[ii][0], starbucks[ii][1], 3000.0)
        else
          # Somewhere off Africa ...
          region = Aerospike::GeoJSON.circle(0.0, 0.0, 3000.0)
        end

        # Accumulate prime factors of the index into a list and map.
        listval = []
        mapval = {}
        [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31].each do |ff|
          if ii >= ff && ii % ff == 0
            listval << ff
            mapval[ff] = sprintf("0x%04x", ff)
          end
        end

        ballast = ("0" * (ii * 16)).force_encoding("binary")

        bins = {
          "intval" => ii,
          "strval" => sprintf("0x%04x", ii),
          "modval" => ii % 10,
          # "locval" => point,
          # "rgnval" => region,
          "lstval" => listval,
          "mapval" => mapval,
          "ballast" => ballast,
        }

        Support.client.put(key, bins, wpolicy)
      end

      tasks = []
      tasks << Support.client.create_index(@namespace, @set, "index_strval", "strval", :string)
      tasks << Support.client.create_index(@namespace, @set, "index_intval", "intval", :numeric)
      tasks.each(&:wait_till_completed)
      expect(tasks.all?(&:completed?)).to be true
    end

    context "Invalid cases" do
      it "should return an error when expression is not boolean" do
        stmt = Aerospike::Statement.new(@namespace, @set)
        stmt.filters << Aerospike::Filter.Range("intval", 0, 400)
        opts = { filter_exp: Aerospike::Exp.int_val(100) }
        expect {
          rs = client.query(stmt, opts)
          rs.each do end
        }.to raise_error (Aerospike::Exceptions::Aerospike) { |error|
          error.result_code == Aerospike::ResultCode::PARAMETER_ERROR
        }
      end
    end

    context "Valid expression" do
      it "should additionally filter indexed query results" do
        stmt = Aerospike::Statement.new(@namespace, @set)
        stmt.filters << Aerospike::Filter.Range("intval", 0, 400)
        opts = { filter_exp: Aerospike::Exp.ge(Aerospike::Exp.int_bin("modval"), Aerospike::Exp.int_val(8)) }

        # The query clause selects [0, 1, ... 400, 401] The predexp
        # only takes mod 8 and 9, should be 2 pre decade or 80 total.

        rs = client.query(stmt, opts)
        count = 0
        rs.each do |rec|
          count += 1
        end
        expect(count).to eq 80
      end

      it "should work for implied scans" do
        stmt = Aerospike::Statement.new(@namespace, @set)
        opts = { filter_exp: Aerospike::Exp.eq(Aerospike::Exp.str_bin("strval"), Aerospike::Exp.str_val("0x0001")) }

        rs = client.query(stmt, opts)
        count = 0
        rs.each do |rec|
          count += 1
        end
        expect(count).to eq 1
      end

      it "expression and or and not must all work" do
        stmt = Aerospike::Statement.new(@namespace, @set)
        opts = { filter_exp: Aerospike::Exp.or(
          Aerospike::Exp.and(
            Aerospike::Exp.not(Aerospike::Exp.eq(Aerospike::Exp.str_bin("strval"), Aerospike::Exp.str_val("0x0001"))),
            Aerospike::Exp.ge(Aerospike::Exp.int_bin("modval"), Aerospike::Exp.int_val(8)),
          ),
          Aerospike::Exp.eq(Aerospike::Exp.str_bin("strval"), Aerospike::Exp.str_val("0x0104")),
          Aerospike::Exp.eq(Aerospike::Exp.str_bin("strval"), Aerospike::Exp.str_val("0x0105")),
          Aerospike::Exp.eq(Aerospike::Exp.str_bin("strval"), Aerospike::Exp.str_val("0x0106")),
        ) }

        rs = client.query(stmt, opts)
        count = 0
        rs.each do |rec|
          count += 1
        end
        expect(count).to eq 203
      end

      it "should query record size" do
        stmt = Aerospike::Statement.new(@namespace, @set)
        stmt.filters << Aerospike::Filter.Range("intval", 1, 10)
        # opts = { filter_exp: Aerospike::Exp.record_size }
        rs = client.query(stmt)
        count = 0
        rs.each do |rec|
          count += 1
        end
        expect(count).to eq 10

      end

    end

    context "for" do
      @namespace = "test"
      @set = "query1000"

      def query_method(exp, ops = {})
        stmt = Aerospike::Statement.new(@namespace, @set)
        ops[:filter_exp] = exp
        rs = client.query(stmt, ops)
        count = 0
        rs.each do |rec|
          count += 1
        end
        count
      end

      before :all do
        Support.client.truncate(@namespace, @set)

        @record_count = 100
        @record_count.times do |ii|
          key = Aerospike::Key.new(@namespace, @set, ii)
          bins = {
            "bin" => ii,
            "bin2" => "#{ii}",
            "bin3" => ii.to_f / 3,
            "bin4" => BytesValue.new("blob#{ii}"),
            "bin5" => ["a", "b", ii],
            "bin6" => { "a": "test", "b": ii },
          }
          Support.client.put(key, bins)
        end
      end

      # [title, result, exp]
      matrix = [
        # data types
        ["int_bin must work", 1, Exp.eq(Exp.int_bin("bin"), Exp.int_val(1))],
        ["str_bin must work", 1, Exp.eq(Exp.str_bin("bin2"), Exp.str_val("1"))],
        ["float_bin must work", 1, Exp.eq(Exp.float_bin("bin3"), Exp.float_val(2))],
        ["blob_bin must work", 1, Exp.eq(Exp.blob_bin("bin4"), Exp.blob_val("blob5"))],
        ["bin_type must work", 100, Exp.ne(Exp.bin_type("bin"), Exp.int_val(0))],
        # logical ops
        ["and must work", 1, Exp.and(Exp.eq(Exp.int_bin("bin"), Exp.int_val(1)), Exp.eq(Exp.str_bin("bin2"), Exp.str_val("1")))],
        ["or must work", 2, Exp.or(Exp.eq(Exp.int_bin("bin"), Exp.int_val(1)), Exp.eq(Exp.int_bin("bin"), Exp.int_val(3)))],
        ["not must work", 99, Exp.not(Exp.eq(Exp.int_bin("bin"), Exp.int_val(1)))],
        # comparisons
        ["eq must work", 1, Exp.eq(Exp.int_bin("bin"), Exp.int_val(1))],
        ["ne must work", 99, Exp.ne(Exp.int_bin("bin"), Exp.int_val(1))],
        ["lt must work", 99, Exp.lt(Exp.int_bin("bin"), Exp.int_val(99))],
        ["le must work", 100, Exp.le(Exp.int_bin("bin"), Exp.int_val(99))],
        ["gt must work", 98, Exp.gt(Exp.int_bin("bin"), Exp.int_val(1))],
        ["ge must work", 99, Exp.ge(Exp.int_bin("bin"), Exp.int_val(1))],
        # record ops
        ["memory_size must work", 100, Exp.ge(Exp.memory_size, Exp.int_val(0))],
        ["last_update must work", 100, Exp.gt(Exp.last_update, Exp.int_val(15000))],
        ["since_update must work", 100, Exp.gt(Exp.since_update, Exp.int_val(150))],
        ["is_tombstone must work", 100, Exp.not(Exp.is_tombstone)],
        ["set_name must work", 100, Exp.eq(Exp.set_name, Exp.str_val("query1000"))],
        ["bin_exists must work", 100, Exp.bin_exists("bin4")],
        ["digest_modulo must work", 34, Exp.eq(Exp.digest_modulo(3), Exp.int_val(1))],
        ["key must work", 0, Exp.eq(Exp.key(Exp::Type::INT), Exp.int_val(50))],
        ["key_exists must work", 0, Exp.key_exists],
        ["nil must work", 100, Exp.eq(Exp.nil_val, Exp.nil_val)],
        ["regex_compare must work", 75, Exp.regex_compare("[1-5]", Exp::RegexFlags::ICASE, Exp.str_bin("bin2"))],
      ]

      matrix.each do |title, result, exp|
        it title do
          expect(query_method(exp)).to eq result
        end
      end
    end

    context "command" do
      before :all do
        @record_count.times do |ii|
          key = Aerospike::Key.new(@namespace, @set, ii)
          bins = { "bin" => ii }
          Support.client.delete(key)
          Support.client.put(key, bins)
        end
      end

      it "should Delete" do
        key = Aerospike::Key.new(@namespace, @set, 15)
        opts = {
          fail_on_filtered_out: true,
          filter_exp: Exp.eq(
            Exp.int_bin("bin"),
            Exp.int_val(16),
          ),
        }
        expect {
          client.delete(key, opts)
        }.to raise_aerospike_error(Aerospike::ResultCode::FILTERED_OUT)

        opts = {
          fail_on_filtered_out: true,
          filter_exp: Exp.eq(
            Exp.int_bin("bin"),
            Exp.int_val(15),
          ),
        }
        client.delete(key, opts)
      end

      it "should Put" do
        key = Aerospike::Key.new(@namespace, @set, 25)
        opts = {
          fail_on_filtered_out: true,
          filter_exp: Exp.eq(
            Exp.int_bin("bin"),
            Exp.int_val(15),
          ),
        }
        expect {
          client.put(key, { "bin" => 26 }, opts)
        }.to raise_aerospike_error(Aerospike::ResultCode::FILTERED_OUT)

        opts = {
          fail_on_filtered_out: true,
          filter_exp: Exp.eq(
            Exp.int_bin("bin"),
            Exp.int_val(25),
          ),
        }
        client.put(key, { "bin" => 26 }, opts)
      end

      it "should Get" do
        key = Aerospike::Key.new(@namespace, @set, 35)
        opts = {
          fail_on_filtered_out: true,
          filter_exp: Exp.eq(
            Exp.int_bin("bin"),
            Exp.int_val(15),
          ),
        }

        expect {
          client.get(key, nil, opts)
        }.to raise_aerospike_error(Aerospike::ResultCode::FILTERED_OUT)

        opts = {
          fail_on_filtered_out: true,
          filter_exp: Exp.eq(
            Exp.int_bin("bin"),
            Exp.int_val(35),
          ),
        }
        client.get(key, ["bin"], opts)
      end

      it "should Exists" do
        key = Aerospike::Key.new(@namespace, @set, 45)
        opts = {
          fail_on_filtered_out: true,
          filter_exp: Exp.eq(
            Exp.int_bin("bin"),
            Exp.int_val(15),
          ),
        }
        expect {
          client.exists(key, opts)
        }.to raise_aerospike_error(Aerospike::ResultCode::FILTERED_OUT)

        opts = {
          fail_on_filtered_out: true,
          filter_exp: Exp.eq(
            Exp.int_bin("bin"),
            Exp.int_val(45),
          ),
        }
        client.exists(key, opts)
      end

      it "should Add" do
        key = Aerospike::Key.new(@namespace, @set, 55)
        opts = {
          fail_on_filtered_out: true,
          filter_exp: Exp.eq(
            Exp.int_bin("bin"),
            Exp.int_val(15),
          ),
        }
        expect {
          client.add(key, { "test55" => "test" }, opts)
        }.to raise_aerospike_error(Aerospike::ResultCode::FILTERED_OUT)

        opts = {
          fail_on_filtered_out: true,
          filter_exp: Exp.eq(
            Exp.int_bin("bin"),
            Exp.int_val(55),
          ),
        }
        client.add(key, { "test55" => "test" }, opts)
      end

      it "should Prepend" do
        key = Aerospike::Key.new(@namespace, @set, 55)
        opts = {
          fail_on_filtered_out: true,
          filter_exp: Exp.eq(
            Exp.int_bin("bin"),
            Exp.int_val(15),
          ),
        }
        expect {
          client.prepend(key, { "test55" => "test" }, opts)
        }.to raise_aerospike_error(Aerospike::ResultCode::FILTERED_OUT)

        opts = {
          fail_on_filtered_out: true,
          filter_exp: Exp.eq(
            Exp.int_bin("bin"),
            Exp.int_val(55),
          ),
        }
        client.prepend(key, { "test55" => "test" }, opts)
      end

      it "should Touch" do
        key = Aerospike::Key.new(@namespace, @set, 65)
        opts = {
          fail_on_filtered_out: true,
          filter_exp: Exp.eq(
            Exp.int_bin("bin"),
            Exp.int_val(15),
          ),
        }
        expect {
          client.touch(key, opts)
        }.to raise_aerospike_error(Aerospike::ResultCode::FILTERED_OUT)

        opts = {
          fail_on_filtered_out: true,
          filter_exp: Exp.eq(
            Exp.int_bin("bin"),
            Exp.int_val(65),
          ),
        }
        client.touch(key, opts)
      end

      it "should Scan" do
        opts = {
          fail_on_filtered_out: true,
          filter_exp: Exp.eq(
            Exp.int_bin("bin"),
            Exp.int_val(75),
          ),
        }

        rs = client.scan_all(@namespace, @set, nil, opts)
        count = 0
        rs.each do |res|
          count += 1
        end
        expect(count).to eq 1
      end
    end

    context "for ops" do
      @namespace = "test"
      @set = "query1000"

      bin_a = "A"
      bin_b = "B"
      bin_c = "C"
      bin_d = "D"
      bin_e = "E"

      key_a = Aerospike::Key.new(@namespace, @set, "A")
      key_b = Aerospike::Key.new(@namespace, @set, Aerospike::BytesValue.new("B"))
      key_c = Aerospike::Key.new(@namespace, @set, "C")

      before :all do
        Support.client.truncate(@namespace, @set)

        Support.client.put(key_a, { bin_a => 1, bin_b => 1.1, bin_c => "abcde", bin_d => 1, bin_e => -1 })
        Support.client.put(key_b, { bin_a => 2, bin_b => 2.2, bin_c => "abcdeabcde", bin_d => 1, bin_e => -2 })
        Support.client.put(key_c, { bin_a => 0, bin_b => -1, bin_c => 1 })
      end

      # [title, exp, key, exp_key, bin, expected, reverse_exp]
      matrix = [
        ["exclusive", Exp.exclusive(Exp.eq(Exp.int_bin(bin_a), Exp.int_val(1)), Exp.eq(Exp.int_bin(bin_d), Exp.int_val(1))), key_a, key_b, bin_a, 2, false],
        ["add_int", Exp.eq(Exp.add(Exp.int_bin(bin_a), Exp.int_bin(bin_d), Exp.int_val(1)), Exp.int_val(4)), key_a, key_b, bin_a, 2, false],
        ["sub_int", Exp.eq(Exp.sub(Exp.int_val(1), Exp.int_bin(bin_a), Exp.int_bin(bin_d)), Exp.int_val(-2)), key_a, key_b, bin_a, 2, false],
        ["mul_int", Exp.eq(Exp.mul(Exp.int_val(2), Exp.int_bin(bin_a), Exp.int_bin(bin_d)), Exp.int_val(4)), key_a, key_b, bin_a, 2, false],
        ["div_int", Exp.eq(Exp.div(Exp.int_val(8), Exp.int_bin(bin_a), Exp.int_bin(bin_d)), Exp.int_val(4)), key_a, key_b, bin_a, 2, false],
        ["mod_int", Exp.eq(Exp.mod(Exp.int_bin(bin_a), Exp.int_val(2)), Exp.int_val(0)), key_a, key_b, bin_a, 2, false],
        ["abs_int", Exp.eq(Exp.abs(Exp.int_bin(bin_e)), Exp.int_val(2)), key_a, key_b, bin_a, 2, false],
        ["floor", Exp.eq(Exp.floor(Exp.float_bin(bin_b)), Exp.float_val(2)), key_a, key_b, bin_a, 2, false],
        ["ceil", Exp.eq(Exp.ceil(Exp.float_bin(bin_b)), Exp.float_val(3)), key_a, key_b, bin_a, 2, false],
        ["to_int", Exp.eq(Exp.to_int(Exp.float_bin(bin_b)), Exp.int_val(2)), key_a, key_b, bin_a, 2, false],
        ["to_float", Exp.eq(Exp.to_float(Exp.int_bin(bin_a)), Exp.float_val(2)), key_a, key_b, bin_a, 2, false],
        ["int_and", Exp.not(
          Exp.and(
            Exp.eq(
              Exp.int_and(Exp.int_bin(bin_a), Exp.int_val(0)),
              Exp.int_val(0)
            ),
            Exp.eq(
              Exp.int_and(Exp.int_bin(bin_a), Exp.int_val(0xFFFF)),
              Exp.int_val(1),
            )
          )
        ), key_a, key_a, bin_a, 1, true],
        ["int_or", Exp.not(
          Exp.and(
            Exp.eq(
              Exp.int_or(Exp.int_bin(bin_a), Exp.int_val(0)),
              Exp.int_val(1)
            ),
            Exp.eq(
              Exp.int_or(Exp.int_bin(bin_a), Exp.int_val(0xFF)),
              Exp.int_val(0xFF),
            )
          )
        ), key_a, key_a, bin_a, 1, true],
        ["int_xor", Exp.not(
          Exp.and(
            Exp.eq(
              Exp.int_xor(Exp.int_bin(bin_a), Exp.int_val(0)),
              Exp.int_val(1)
            ),
            Exp.eq(
              Exp.int_xor(Exp.int_bin(bin_a), Exp.int_val(0xFF)),
              Exp.int_val(0xFE),
            )
          )
        ), key_a, key_a, bin_a, 1, true],
        ["int_not", Exp.not(
          Exp.eq(
            Exp.int_not(Exp.int_bin(bin_a)),
            Exp.int_val(-2)
          )
        ), key_a, key_a, bin_a, 1, true],
        ["lshift", Exp.not(
          Exp.eq(
            Exp.lshift(Exp.int_bin(bin_a), Exp.int_val(2)),
            Exp.int_val(4)
          )
        ), key_a, key_a, bin_a, 1, true],
        ["rshift", Exp.not(
          Exp.eq(
            Exp.rshift(Exp.int_bin(bin_e), Exp.int_val(62)),
            Exp.int_val(3)
          )
        ), key_b, key_b, bin_e, -2, true],
        ["arshift", Exp.not(
          Exp.eq(
            Exp.arshift(Exp.int_bin(bin_e), Exp.int_val(62)),
            Exp.int_val(-1)
          )
        ), key_b, key_b, bin_e, -2, true],
        ["bit_count", Exp.not(
          Exp.eq(
            Exp.count(Exp.int_bin(bin_a)),
            Exp.int_val(1)
          )
        ), key_a, key_a, bin_a, 1, true],
        ["lscan", Exp.not(
          Exp.eq(
            Exp.lscan(Exp.int_bin(bin_a), Exp.bool_val(true)),
            Exp.int_val(63)
          )
        ), key_a, key_a, bin_a, 1, true],
        ["rscan", Exp.not(
          Exp.eq(
            Exp.rscan(Exp.int_bin(bin_a), Exp.bool_val(true)),
            Exp.int_val(63)
          )
        ), key_a, key_a, bin_a, 1, true],
        ["min", Exp.not(
          Exp.eq(
            Exp.min(Exp.int_bin(bin_a), Exp.int_bin(bin_d), Exp.int_bin(bin_e)),
            Exp.int_val(-1)
          )
        ), key_a, key_a, bin_a, 1, true],
        ["max", Exp.not(
          Exp.eq(
            Exp.max(Exp.int_bin(bin_a), Exp.int_bin(bin_d), Exp.int_bin(bin_e)),
            Exp.int_val(1)
          )
        ), key_a, key_a, bin_a, 1, true],
        ["cond", Exp.not(
          Exp.eq(
            Exp.cond(
              Exp.eq(Exp.int_bin(bin_a), Exp.int_val(0)), Exp.add(Exp.int_bin(bin_d), Exp.int_bin(bin_e)),
              Exp.eq(Exp.int_bin(bin_a), Exp.int_val(1)), Exp.sub(Exp.int_bin(bin_d), Exp.int_bin(bin_e)),
              Exp.eq(Exp.int_bin(bin_a), Exp.int_val(2)), Exp.mul(Exp.int_bin(bin_d), Exp.int_bin(bin_e)),
              Exp.int_val(-1)
            ),
            Exp.int_val(2)
          )
        ), key_a, key_a, bin_a, 1, true],

        ["add_float", Exp.let(
          Exp.def("val", Exp.add(Exp.float_bin(bin_b), Exp.float_val(1.1))),
          Exp.and(
            Exp.ge(Exp.var("val"), Exp.float_val(3.2999)),
            Exp.le(Exp.var("val"), Exp.float_val(3.3001)),
          )
        ),
         key_a, key_b, bin_a, 2, false],
        ["log_float", Exp.let(
          Exp.def("val", Exp.log(Exp.float_bin(bin_b), Exp.float_val(2.0))),
          Exp.and(
            Exp.ge(Exp.var("val"), Exp.float_val(1.1374)),
            Exp.le(Exp.var("val"), Exp.float_val(1.1376))
          )
        ), key_a, key_b, bin_a, 2, false],
        ["pow_float", Exp.let(
          Exp.def("val", Exp.pow(Exp.float_bin(bin_b), Exp.float_val(2.0))),
          Exp.and(
            Exp.ge(Exp.var("val"), Exp.float_val(4.8399)),
            Exp.le(Exp.var("val"), Exp.float_val(4.8401))
          )
        ), key_a, key_b, bin_a, 2, false],
      ]

      matrix.each do |title, exp, key, exp_key, bin, expected, reverse_exp|
        it "#{title} should work" do
          opts = {
            fail_on_filtered_out: true,
            filter_exp: exp,
          }

          expect {
            client.get(key, nil, opts)
          }.to raise_error (Aerospike::Exceptions::Aerospike) { |error|
            error.result_code == Aerospike::ResultCode::FILTERED_OUT
          }

          opts = {
            fail_on_filtered_out: true,
            filter_exp: Exp.not(exp),
          } if reverse_exp
          r = client.get(exp_key, nil, opts)
          client.get(key)
          expect(r.bins[bin]).to eq expected
        end
      end
    end
  end # describe
end # describe
