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

require "spec_helper"
require "aerospike/query/statement"

describe Aerospike::Client do

  describe "Query operations" do

    before :all do

      @client = described_class.new(Support.host, Support.port, :user => Support.user, :password => Support.password)
      @record_count = 1000

      for i in 1..@record_count
        key = Aerospike::Key.new('test', 'test998', i)

        bin_map = {
          'bin1' => "value#{i}",
          'bin2' => i,
          'bin4' => ['value4', {'map1' => 'map val'}],
          'bin5' => {'value5' => [124, "string value"]},
        }

        @client.put(key, bin_map)

        expect(@client.exists(key)).to eq true
      end

      index_task = @client.create_index(
        key.namespace,
        key.set_name,
        "index_int_bin2",
        'bin2', :numeric
        )

      expect(index_task.wait_till_completed).to be true
      expect(index_task.completed?).to be true

      index_task = @client.create_index(
        key.namespace,
        key.set_name,
        "index_str_bin1",
        'bin1', :string
        )

      expect(index_task.wait_till_completed).to be true
      expect(index_task.completed?).to be true
    end

    after :all do
      @client.close
    end

    context "No Filter == Scan" do

      it "should return all records" do
        rs = @client.query(Aerospike::Statement.new('test', 'test998', ))

        i = 0
        rs.each do |rec|
          i +=1
          expect(rec.bins['bin1']).to eq "value#{rec.bins['bin2']}"
        end

        expect(i).to eq @record_count

      end # it

    end # context

    context "Equal Filter" do

      context "Numeric Bins" do

        it "should return relevent records" do
          stmt = Aerospike::Statement.new('test', 'test998', ['bin1', 'bin2'])
          stmt.filters = [Aerospike::Filter.Equal('bin2', 1)]
          rs = @client.query(stmt)

          i = 0
          rs.each do |rec|
            i +=1
            expect(rec.bins['bin1']).to eq "value#{rec.bins['bin2']}"
            expect(rec.bins.length).to eq 2
          end

          expect(i).to eq 1

        end # it

      end # context

      context "String Bins" do

        it "should return relevent records" do
          stmt = Aerospike::Statement.new('test', 'test998')
          stmt.filters = [Aerospike::Filter.Equal('bin1', 'value1')]
          rs = @client.query(stmt)

          i = 0
          rs.each do |rec|
            i +=1
            expect(rec.bins['bin1']).to eq "value#{rec.bins['bin2']}"
          end

          expect(i).to eq 1

        end # it

      end # context

    end # context

    context "Range Filter" do

      context "Numeric Bins" do

        it "should return relevent records" do
          stmt = Aerospike::Statement.new('test', 'test998')
          stmt.filters = [Aerospike::Filter.Range('bin2', 10, 100)]
          rs = @client.query(stmt)

          i = 0
          rs.each do |rec|
            i +=1
            expect(rec.bins['bin1']).to eq "value#{rec.bins['bin2']}"
          end

          expect(i).to eq 91

        end # it

      end # context

    end # context

  end # describe

end # describe
