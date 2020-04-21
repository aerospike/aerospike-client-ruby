# Copyright 2014-2018 Aerospike, Inc.
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

require 'aerospike'
require 'benchmark'

describe Aerospike::Client do

  describe "Predicates" do

    let(:key) { Aerospike::Key.new(Support.namespace, 'predexp_ops_spec', 0) }
    let(:client) { Support.client }
    let(:valid_predicate) { 
      [
        Aerospike::PredExp.integer_bin('bin2'),
        Aerospike::PredExp.integer_value(9),
        Aerospike::PredExp.integer_less_eq
      ]
     }

    let(:invalid_predicate) { 
      [
        Aerospike::PredExp.string_bin('bin1'),
        Aerospike::PredExp.string_value('value'),
        Aerospike::PredExp.string_unequal
      ]
     }

    before :each do
      client.delete(key)
      client.put(key, {'bin1' => 'value', 'bin2' => 9})
    end

    describe "#put" do

      it "should put the key if the predicate is valid" do
        client.put(key, {'bin3' => 1.1}, predexp: valid_predicate)
        rec = client.get(key)
        expect(rec.bins['bin1']).to eq 'value'
        expect(rec.bins['bin2']).to eq 9
        expect(rec.bins['bin3']).to eq 1.1
      end

      it "should NOT put the key if the predicate is invalid" do
        client.put(key, {'bin3' => 1.1}, predexp: invalid_predicate)
        rec = client.get(key)
        expect(rec.bins['bin1']).to eq 'value'
        expect(rec.bins['bin2']).to eq 9
        expect(rec.bins['bin3']).to be_nil
      end

      it "should raise exception if the predicate is invalid" do
        expect { 
          client.put(key, {'bin3' => 1.1}, predexp: invalid_predicate, fail_on_filtered_out: true)
        }.to raise_error (Aerospike::Exceptions::Aerospike){ |error|
          error.result_code == Aerospike::ResultCode::FILTERED_OUT
        }
      end

    end

    describe "#get" do

      it "should get the key if the predicate is valid" do
        rec = client.get(key, [], predexp: valid_predicate)
        expect(rec.bins['bin1']).to eq 'value'
        expect(rec.bins['bin2']).to eq 9
      end

      it "should NOT get the key if the predicate is invalid" do
        rec = client.get(key, [], predexp: invalid_predicate)
        expect(rec).to be_nil
      end

      it "should raise exception if the predicate is invalid" do
        expect { 
          client.get(key, [], predexp: invalid_predicate, fail_on_filtered_out: true)
        }.to raise_error (Aerospike::Exceptions::Aerospike){ |error|
          error.result_code == Aerospike::ResultCode::FILTERED_OUT
        }
      end

    end

    describe "#get_header" do

      it "should get the key if the predicate is valid" do
        rec = client.get_header(key, predexp: valid_predicate)
        expect(rec).not_to be_nil
      end

      it "should NOT get the key if the predicate is invalid" do
        rec = client.get_header(key, predexp: invalid_predicate)
        expect(rec).to be_nil
      end

      it "should raise exception if the predicate is invalid" do
        expect { 
          client.get_header(key, predexp: invalid_predicate, fail_on_filtered_out: true)
        }.to raise_error (Aerospike::Exceptions::Aerospike){ |error|
          error.result_code == Aerospike::ResultCode::FILTERED_OUT
        }
      end

    end

    describe "#delete" do

      it "should delete a key if the predicate is valid" do
        existed = client.delete(key, predexp: valid_predicate)
        expect(existed).to eq true
      end

      it "should NOT delete a key if the predicate is invalid" do
        existed = client.delete(key, predexp: invalid_predicate)
        expect(existed).to eq true
        rec = client.get(key, [])
        expect(rec.bins['bin1']).to eq 'value'
        expect(rec.bins['bin2']).to eq 9
      end

      it "should raise exception if the predicate is invalid" do
        expect { 
          client.delete(key, predexp: invalid_predicate, fail_on_filtered_out: true)
        }.to raise_error (Aerospike::Exceptions::Aerospike){ |error|
          error.result_code == Aerospike::ResultCode::FILTERED_OUT
        }
      end

    end

    describe "#touch" do

      it "should touch the record to bump its generation if the predicate is valid" do
        client.touch(key, predexp: valid_predicate)
        record = client.get_header(key)
        expect(record.generation).to eq 2
      end

      it "should NOT touch the record to bump its generation if the predicate is invalid" do
        client.touch(key, predexp: invalid_predicate)
        record = client.get_header(key)
        expect(record.generation).to eq 1
      end

      it "should raise exception if the predicate is invalid" do
        expect { 
          client.touch(key, predexp: invalid_predicate, fail_on_filtered_out: true)
        }.to raise_error (Aerospike::Exceptions::Aerospike){ |error|
          error.result_code == Aerospike::ResultCode::FILTERED_OUT
        }
      end

    end

    describe "#exists" do

      it "should check existence of the record if the predicate is valid" do
        existed = client.exists(key, predexp: valid_predicate)
        expect(existed).to eq true
      end

      it "should NOT check existence of the record if the predicate is invalid" do
        existed = client.exists(key, predexp: invalid_predicate)
        expect(existed).to eq true
      end

      it "should raise exception if the predicate is invalid" do
        expect { 
          client.exists(key, predexp: invalid_predicate, fail_on_filtered_out: true)
        }.to raise_error (Aerospike::Exceptions::Aerospike){ |error|
          error.result_code == Aerospike::ResultCode::FILTERED_OUT
        }
      end

    end

    describe "#operate" do

      let(:bin_int) do
        Aerospike::Bin.new('bin2', 5)
      end

      it "should #add, #get if the predicate is valid" do
        client.operate(key, [
                         Aerospike::Operation.add(bin_int),
        ], predexp: valid_predicate)
        rec = client.get(key)
        expect(rec.bins[bin_int.name]).to eq bin_int.value + 9
        expect(rec.generation).to eq 2
      end

      it "should NOT #add, #get if the predicate is invalid" do
        client.operate(key, [
                         Aerospike::Operation.add(bin_int),
        ], predexp: invalid_predicate)
        rec = client.get(key)
        expect(rec.bins[bin_int.name]).to eq 9
        expect(rec.generation).to eq 1
      end

      it "should raise exception if the predicate is invalid" do
        expect { 
        client.operate(key, [
                         Aerospike::Operation.add(bin_int),
        ], predexp: invalid_predicate, fail_on_filtered_out: true)
        }.to raise_error (Aerospike::Exceptions::Aerospike){ |error|
          error.result_code == Aerospike::ResultCode::FILTERED_OUT
        }
      end

    end

    describe "#batch" do

      it "should batch_get if the predicate is valid" do
        result = client.batch_get([key], [], predexp: valid_predicate)
        expect(result[0].bins['bin1']).to eq 'value'
      end

      it "should NOT batch_get if the predicate is invalid" do
        result = client.batch_get([key], [], predexp: invalid_predicate)
        expect(result[0]).to be_nil
      end

    end

    describe "#scan" do

      it "should scan and return records if the predicate is valid" do
        rs = client.scan_all(key.namespace, key.set_name, nil, predexp: valid_predicate)
        count = 0
        rs.each do |rs|
          count += 1
        end

        expect(count).to eq 1
      end

      it "should NOT scan and return records if the predicate is invalid" do
        rs = client.scan_all(key.namespace, key.set_name, nil, predexp: invalid_predicate)
        count = 0
        rs.each do |rs|
          count += 1
        end

        expect(count).to eq 0
      end

    end

    describe "#query" do

      let(:stmt) { stmt = Aerospike::Statement.new(key.namespace, key.set_name) }

      it "should query and return records if the predicate is valid" do
        stmt.predexp = valid_predicate
        rs = client.query(stmt)
        count = 0
        rs.each do |rs|
          count += 1
        end

        expect(count).to eq 1
      end

      it "should query and return records if the predicate is invalid - predexp on policy" do
        rs = client.query(stmt, predexp: valid_predicate)
        count = 0
        rs.each do |rs|
          count += 1
        end

        expect(count).to eq 1
      end

      it "should query and return records if the predicate is valid - predexp on policy" do
        # policy value takes precedence
        stmt.predexp = invalid_predicate
        rs = client.query(stmt, predexp: valid_predicate)
        count = 0
        rs.each do |rs|
          count += 1
        end

        expect(count).to eq 1
      end

      it "should NOT query and return records if the predicate is valid - predexp on policy" do
        # policy value takes precedence
        stmt.predexp = invalid_predicate
        rs = client.query(stmt)
        count = 0
        rs.each do |rs|
          count += 1
        end

        expect(count).to eq 0
      end

      it "should NOT query and return records if the predicate is valid - predexp on policy" do
        rs = client.query(stmt, predexp: invalid_predicate)
        count = 0
        rs.each do |rs|
          count += 1
        end

        expect(count).to eq 0
      end

    end

  end

end
