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

describe Aerospike::Exp::Operation do
  let(:client) { Support.client }

  describe "Expression Operations" do
    before do
      client.default_operate_policy.record_bin_multiplicity = RecordBinMultiplicity::ARRAY
    end

    after do
      client.default_operate_policy.record_bin_multiplicity = RecordBinMultiplicity::SINGLE
    end

    before :each do
      @namespace = "test"
      @set = "test"

      @bin_a = "A"
      @bin_b = "B"
      @bin_c = "C"
      @bin_d = "D"
      @bin_h = "H"
      @exp_var = "EV"

      @key_a = Aerospike::Key.new(@namespace, @set, "A")
      @key_b = Aerospike::Key.new(@namespace, @set, Aerospike::BytesValue.new("B"))

      client.delete(@key_a)
      client.delete(@key_b)

      client.put(@key_a, { @bin_a => 1, @bin_d => 2 })
      client.put(@key_b, { @bin_b => 2, @bin_d => 2 })
    end

    def bytes_to_str(bytes)
      bytes.pack("C*").force_encoding("binary")
    end

    # it "Expression ops on lists should work" do
    #   list = Aerospike::ListValue.new([Aerospike::StringValue.new("a"), Aerospike::StringValue.new("b"), Aerospike::StringValue.new("c"), Aerospike::StringValue.new("d")])
    #   exp = Aerospike::Exp::list_val(list)
    #   rec = client.operate(@key_a, [Aerospike::Exp::Operation.write(@bin_c, exp),
    #                                 Aerospike::Operation::get(@bin_c),
    #                                 Aerospike::Exp::Operation.read("var", exp)])

    #   expected = { "C" => [nil, ["a", "b", "c", "d"]], "var" => ["a", "b", "c", "d"] }
    #   expect(rec.bins).to eq expected
    # end

    it "Read Eval error should work" do
      exp = Aerospike::Exp::add(Aerospike::Exp::int_bin(@bin_a), Aerospike::Exp::int_val(4))

      r = client.operate(@key_a, [Aerospike::Exp::Operation.read(@exp_var, exp)])
      expect(r.bins&.length).to be > 0

      expect {
        client.operate(@key_b, [Aerospike::Exp::Operation.read(@exp_var, exp)])
      }.to raise_error (Aerospike::Exceptions::Aerospike) { |error|
        error.result_code == Aerospike::ResultCode::OP_NOT_APPLICABLE
      }

      r = client.operate(@key_b, [Aerospike::Exp::Operation.read(@exp_var, exp, Aerospike::Exp::ReadFlags::EVAL_NO_FAIL)])
      expect(r.bins&.length).to be > 0
    end

    it "Read On Write Eval error should work" do
      rexp = Aerospike::Exp::int_bin(@bin_d)
      wexp = Aerospike::Exp::int_bin(@bin_a)

      r = client.operate(@key_a, [
        Aerospike::Exp::Operation.write(@bin_d, wexp),
        Aerospike::Exp::Operation.read(@exp_var, rexp),
      ])
      expect(r.bins&.length).to be > 0

      expect {
        client.operate(@key_b, [
          Aerospike::Exp::Operation.write(@bin_d, wexp),
          Aerospike::Exp::Operation.read(@exp_var, rexp),
        ])
      }.to raise_error (Aerospike::Exceptions::Aerospike) { |error|
        error.result_code == Aerospike::ResultCode::OP_NOT_APPLICABLE
      }

      r = client.operate(@key_b, [
        Aerospike::Exp::Operation.write(@bin_d, wexp, Aerospike::Exp::WriteFlags::EVAL_NO_FAIL),
        Aerospike::Exp::Operation.read(@exp_var, rexp, Aerospike::Exp::ReadFlags::EVAL_NO_FAIL),
      ])
      expect(r.bins&.length).to be > 0
    end

    it "Write Eval error should work" do
      wexp = Aerospike::Exp::add(Aerospike::Exp::int_bin(@bin_a), Aerospike::Exp::int_val(4))
      rexp = Aerospike::Exp::int_bin(@bin_c)

      r = client.operate(@key_a, [
        Aerospike::Exp::Operation.write(@bin_c, wexp),
        Aerospike::Exp::Operation.read(@exp_var, rexp),
      ])
      expect(r.bins&.length).to be > 0

      expect {
        client.operate(@key_b, [
          Aerospike::Exp::Operation.write(@bin_c, wexp),
          Aerospike::Exp::Operation.read(@exp_var, rexp),
        ])
      }.to raise_error (Aerospike::Exceptions::Aerospike) { |error|
        error.result_code == Aerospike::ResultCode::OP_NOT_APPLICABLE
      }

      r = client.operate(@key_b, [
        Aerospike::Exp::Operation.write(@bin_c, wexp, Aerospike::Exp::WriteFlags::EVAL_NO_FAIL),
        Aerospike::Exp::Operation.read(@exp_var, rexp, Aerospike::Exp::ReadFlags::EVAL_NO_FAIL),
      ])
      expect(r.bins&.length).to be > 0
    end

    it "Write Policy error should work" do
      wexp = Aerospike::Exp::add(Aerospike::Exp::int_bin(@bin_a), Aerospike::Exp::int_val(4))

      expect {
        client.operate(@key_a, [
          Aerospike::Exp::Operation.write(@bin_c, wexp, Aerospike::Exp::WriteFlags::UPDATE_ONLY),
        ])
      }.to raise_error (Aerospike::Exceptions::Aerospike) { |error|
        error.result_code == Aerospike::ResultCode::BIN_NOT_FOUND
      }

      r = client.operate(@key_a, [
        Aerospike::Exp::Operation.write(@bin_c, wexp, Aerospike::Exp::WriteFlags::UPDATE_ONLY | Aerospike::Exp::WriteFlags::POLICY_NO_FAIL),
      ])
      expect(r.bins&.length).to be > 0

      client.operate(@key_a, [
        Aerospike::Exp::Operation.write(@bin_c, wexp, Aerospike::Exp::WriteFlags::CREATE_ONLY),
      ])
      expect(r.bins&.length).to be > 0

      expect {
        client.operate(@key_a, [
          Aerospike::Exp::Operation.write(@bin_c, wexp, Aerospike::Exp::WriteFlags::CREATE_ONLY),
        ])
      }.to raise_error (Aerospike::Exceptions::Aerospike) { |error|
        error.result_code == Aerospike::ResultCode::BIN_EXISTS_ERROR
      }

      r = client.operate(@key_a, [
        Aerospike::Exp::Operation.write(@bin_c, wexp, Aerospike::Exp::WriteFlags::UPDATE_ONLY | Aerospike::Exp::WriteFlags::POLICY_NO_FAIL),
      ])
      expect(r.bins&.length).to be > 0

      dexp = Aerospike::Exp::nil_val

      expect {
        client.operate(@key_a, [
          Aerospike::Exp::Operation.write(@bin_c, dexp),
        ])
      }.to raise_error (Aerospike::Exceptions::Aerospike) { |error|
        error.result_code == Aerospike::ResultCode::OP_NOT_APPLICABLE
      }

      r = client.operate(@key_a, [
        Aerospike::Exp::Operation.write(@bin_c, dexp, Aerospike::Exp::WriteFlags::POLICY_NO_FAIL),
      ])
      expect(r.bins&.length).to be > 0

      r = client.operate(@key_a, [
        Aerospike::Exp::Operation.write(@bin_c, dexp, Aerospike::Exp::WriteFlags::ALLOW_DELETE),
      ])
      expect(r.bins&.length).to be > 0

      r = client.operate(@key_a, [
        Aerospike::Exp::Operation.write(@bin_c, wexp, Aerospike::Exp::WriteFlags::CREATE_ONLY),
      ])
      expect(r.bins&.length).to be > 0
    end

    it "Return Unknown should work" do
      exp = Aerospike::Exp::cond(
        Aerospike::Exp::eq(Aerospike::Exp::int_bin(@bin_c), Aerospike::Exp::int_val(5)), Aerospike::Exp::unknown,
        Aerospike::Exp::bin_exists(@bin_a), Aerospike::Exp::int_val(5),
        Aerospike::Exp::unknown,
      )

      expect {
        r = client.operate(@key_a, [
          Aerospike::Exp::Operation.write(@bin_c, exp),
          Aerospike::Operation::get(@bin_c),
        ])
      }.to raise_error (Aerospike::Exceptions::Aerospike) { |error|
        error.result_code == Aerospike::ResultCode::OP_NOT_APPLICABLE
      }

      r = client.operate(@key_b, [
        Aerospike::Exp::Operation.write(@bin_c, exp, Aerospike::Exp::WriteFlags::EVAL_NO_FAIL),
        Aerospike::Operation::get(@bin_c),
      ])
      expect(r.bins&.length).to be > 0

      expected = { @bin_c => [nil, nil] }
      expect(r.bins).to eq expected
    end

    it "Return Nil should work" do
      exp = Aerospike::Exp::nil_val

      r = client.operate(@key_a, [
        Aerospike::Exp::Operation.read(@exp_var, exp),
        Aerospike::Operation.get(@bin_c),
      ])

      expected = { @exp_var => nil, @bin_c => nil }
      expect(r.bins).to eq expected
    end

    it "Return Int should work" do
      exp = Aerospike::Exp::add(Aerospike::Exp::int_bin(@bin_a), Aerospike::Exp::int_val(4))

      r = client.operate(@key_a, [
        Aerospike::Exp::Operation.write(@bin_c, exp),
        Aerospike::Operation.get(@bin_c),
        Aerospike::Exp::Operation.read(@exp_var, exp),
      ])

      expected = { @exp_var => 5, @bin_c => [nil, 5] }
      expect(r.bins).to eq expected

      r = client.operate(@key_a, [
        Aerospike::Exp::Operation.read(@exp_var, exp),
      ])

      expected = { @exp_var => 5 }
      expect(r.bins).to eq expected
    end

    it "Return Float should work" do
      exp = Aerospike::Exp::add(Aerospike::Exp::to_float(Aerospike::Exp::int_bin(@bin_a)), Aerospike::Exp::float_val(4.1))

      r = client.operate(@key_a, [
        Aerospike::Exp::Operation.write(@bin_c, exp),
        Aerospike::Operation.get(@bin_c),
        Aerospike::Exp::Operation.read(@exp_var, exp),
      ])

      expected = { @exp_var => 5.1, @bin_c => [nil, 5.1] }
      expect(r.bins).to eq expected

      r = client.operate(@key_a, [
        Aerospike::Exp::Operation.read(@exp_var, exp),
      ])

      expected = { @exp_var => 5.1 }
      expect(r.bins).to eq expected
    end

    it "Return String should work" do
      str = "xxx"
      exp = Aerospike::Exp::str_val(str)

      r = client.operate(@key_a, [
        Aerospike::Exp::Operation.write(@bin_c, exp),
        Aerospike::Operation.get(@bin_c),
        Aerospike::Exp::Operation.read(@exp_var, exp),
      ])

      expected = { @exp_var => str, @bin_c => [nil, str] }
      expect(r.bins).to eq expected

      r = client.operate(@key_a, [
        Aerospike::Exp::Operation.read(@exp_var, exp),
      ])

      expected = { @exp_var => str }
      expect(r.bins).to eq expected
    end

    it "Return BLOB should work" do
      blob = bytes_to_str([0x78, 0x78, 0x78])
      exp = Aerospike::Exp::blob_val(blob)

      r = client.operate(@key_a, [
        Aerospike::Exp::Operation.write(@bin_c, exp),
        Aerospike::Operation.get(@bin_c),
        Aerospike::Exp::Operation.read(@exp_var, exp),
      ])

      expected = { @exp_var => blob, @bin_c => [nil, blob] }
      expect(r.bins).to eq expected

      r = client.operate(@key_a, [
        Aerospike::Exp::Operation.read(@exp_var, exp),
      ])

      expected = { @exp_var => blob }
      expect(r.bins).to eq expected
    end

    it "Return Boolean should work" do
      exp = Aerospike::Exp::eq(Aerospike::Exp::int_bin(@bin_a), Aerospike::Exp::int_val(1))

      r = client.operate(@key_a, [
        Aerospike::Exp::Operation.write(@bin_c, exp),
        Aerospike::Operation.get(@bin_c),
        Aerospike::Exp::Operation.read(@exp_var, exp),
      ])

      expected = { @exp_var => true, @bin_c => [nil, true] }
      expect(r.bins).to eq expected
    end

    it "Return HLL should work" do
      exp = Aerospike::Exp::HLL.init(Aerospike::Exp::int_val(4), Aerospike::Exp::nil_val)

      r = client.operate(@key_a, [
        Aerospike::CDT::HLLOperation.init(@bin_h, 4, -1),
        Aerospike::Exp::Operation.write(@bin_c, exp),
        Aerospike::Operation.get(@bin_h),
        Aerospike::Operation.get(@bin_c),
        Aerospike::Exp::Operation.read(@exp_var, exp),
      ])

      expected = {
        @bin_h => [
          nil,
          Aerospike::HLLValue.new(bytes_to_str([0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])),
        ],
        @bin_c => [
          nil,
          Aerospike::HLLValue.new(bytes_to_str([0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])),
        ],
        @exp_var => Aerospike::HLLValue.new(bytes_to_str([0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])),
      }
      expect(r.bins).to eq expected

      r = client.operate(@key_a, [Aerospike::Exp::Operation.read(@exp_var, exp)])
      expected = { @exp_var => bytes_to_str([0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]) }
      expect(r.bins).to eq expected
    end
  end
end
