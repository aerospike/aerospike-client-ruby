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

require 'aerospike'
require 'aerospike/batch_read'

describe Aerospike::Client do

  let(:client) { Support.client }

  describe "#batch_operate" do
    let(:batch_policy) do
      Aerospike::BatchPolicy.new
    end

    let(:existing_keys) { Array.new(3) { Support.gen_random_key } }
    # let(:existing_keys) { [Aerospike::Key.new("test", "test", 1)] }
    let(:keys) { existing_keys }
    let(:no_such_key) { Support.gen_random_key }
    let(:keys) { existing_keys }
    let(:opts) { { filter_exp: Aerospike::Exp.eq(Aerospike::Exp.int_bin("strval"), Aerospike::Exp.int_val(0)) } }

    before do
      existing_keys.each_with_index do |key, idx|
        client.put(key, {
                     'idx' => idx,
                     'key' => key.user_key,
                     'rnd' => 99 #rand
                   }, {})
      end
    end

    context '#BatchRead' do
      it 'returns specified bins' do
        bin_names = %w[idx rnd]
        records = [Aerospike::BatchRead.read_bins(keys.first, bin_names)]
        client.batch_operate(records, batch_policy)

        expect(records[0].result_code).to eql 0
        expect(records[0].record.bins.length).to eql 2
      end

      it 'returns all records' do
        records = [Aerospike::BatchRead.read_all_bins(keys.first)]
        client.batch_operate(records, batch_policy)

        expect(records[0].result_code).to eql 0
        expect(records[0].record.bins.length).to eql 3
      end

      it 'filter out' do
        records = [Aerospike::BatchRead.read_all_bins(keys.first)]
        client.batch_operate(records, opts)

        expect(records[0].result_code).to eql Aerospike::ResultCode::FILTERED_OUT
        expect(records[0].record).to eql nil
      end
    end

    context '#BatchWrite' do
      it 'updates specified bins' do
        ops = [
          Aerospike::Operation.put(Aerospike::Bin.new("new_bin_str", "value")),
          Aerospike::Operation.put(Aerospike::Bin.new("new_bin_int", 999)),
          Aerospike::Operation.add(Aerospike::Bin.new("new_bin_int", 1))
        ]
        records = [Aerospike::BatchWrite.new(keys.first, ops)]
        client.batch_operate(records, batch_policy)

        expect(records[0].result_code).to eql 0
        expect(records[0].record.bins).to eql({ "new_bin_int"=>nil, "new_bin_str"=>nil })

        records = [Aerospike::BatchRead.read_all_bins(keys.first)]
        client.batch_operate(records, batch_policy)

        expect(records[0].record.bins).to eql({
                                                'idx' => 0,
                                                'key' => keys.first.user_key,
                                                'rnd' => 99, #rand
                                                "new_bin_str" => "value",
                                                "new_bin_int" => 1000
                                              })
      end

      it 'filter out' do
        ops = [
          Aerospike::Operation.put(Aerospike::Bin.new("new_bin_str", "value")),
          Aerospike::Operation.put(Aerospike::Bin.new("new_bin_int", 999)),
          Aerospike::Operation.add(Aerospike::Bin.new("new_bin_int", 1))
        ]
        records = [Aerospike::BatchWrite.new(keys.first, ops)]
        client.batch_operate(records, opts)

        expect(records[0].result_code).to eql Aerospike::ResultCode::FILTERED_OUT
        expect(records[0].record).to eql nil
      end

      it 'removes specific records' do
        ops = [
          Aerospike::Operation.delete
        ]
        records = [Aerospike::BatchWrite.new(keys.first, ops)]
        client.batch_operate(records, batch_policy)

        exists = client.exists(keys.first)
        expect(exists).to eql false

      end
    end

    context '#BatchDelete' do
      it 'removes specific records' do
        ops = [
          Aerospike::Operation.delete
        ]
        records = [Aerospike::BatchDelete.new(keys.first)]
        client.batch_operate(records, batch_policy)

        expect(records[0].result_code).to eql 0
        expect(records[0].record.bins).to eql nil

        exists = client.exists(keys.first)
        expect(exists).to eql false
      end

      it 'filter out' do
        ops = [
          Aerospike::Operation.delete
        ]
        records = [Aerospike::BatchDelete.new(keys.first)]
        client.batch_operate(records, opts)

        expect(records[0].result_code).to eql Aerospike::ResultCode::FILTERED_OUT
        expect(records[0].record).to eql nil
      end

    end # context

    context '#BatchUDF' do
      let(:udf_body_string) do
        "function testStr(rec, str)
          return str                              -- Return the Return value and/or status
        end"
      end

      before do
        register_task = client.register_udf(udf_body_string, "test-udf-batch.lua", Aerospike::Language::LUA)
        expect(register_task.wait_till_completed).to be true
        expect(register_task.completed?).to be true
      end

      it 'calls specific UDF' do
        records = [Aerospike::BatchUDF.new(keys.first, "test-udf-batch", "testStr", ["ping_str"])]
        client.batch_operate(records, batch_policy)

        expect(records[0].result_code).to eql 0
        expect(records[0].record.bins).to eql({ "SUCCESS"=>"ping_str" })
      end

      it 'filter out' do
        records = [Aerospike::BatchUDF.new(keys.first, "test-udf-batch", "testStr", ["ping_str"])]
        client.batch_operate(records, opts)

        expect(records[0].result_code).to eql Aerospike::ResultCode::FILTERED_OUT
        expect(records[0].record).to eql nil
      end

    end # context

  end # describe

end # describe
