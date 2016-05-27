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

require 'aerospike/host'
require 'aerospike/key'
require 'aerospike/bin'
require 'aerospike/language'

describe Aerospike::Client do

  describe "UDF operations" do

    let(:client) { Support.client }

    let(:udf_body) do
      "function testFunc1(rec, div, str)
         local ret = map                     -- Initialize the return value (a map)
         local x = rec['bin1']                 -- Get the value from record bin named 'bin1'
         rec['bin2'] = (x / div)               -- Set the value in record bin named 'bin2'
         aerospike:update(rec)                 -- Update the main record
         ret['status'] = 'OK'                   -- Populate the return status
         return ret                             -- Return the Return value and/or status
      end"
    end

    let(:udf_body_string) do
      "function testStr(rec, str)
         return str                              -- Return the Return value and/or status
      end"
    end

    let(:udf_body_delete) do
      @udf_body_delete = "function delete_record(rec)
         aerospike:remove(rec)                   -- Delete main record, Populate the return status
      end"
    end

    let(:udf_body_send_key) do
      @udf_body_delete = "function create_record(rec, bin, value)
        rec[bin] = value;
        aerospike:create(rec);
      end"
    end

    it "should register UDFs, list them and and then successfully drop them" do
      2.times do |i|
        register_task = client.register_udf(udf_body, "test-udf#{i}.lua", Aerospike::Language::LUA)
        expect(register_task.wait_till_completed).to be true
        expect(register_task.completed?).to be true
      end

      # should list the udfs
      modules = client.list_udf.map(&:filename)
      expect(modules).to include("test-udf0.lua", "test-udf1.lua")

      2.times do |i|
        remove_task = client.remove_udf("test-udf#{i}.lua")
        expect(remove_task.wait_till_completed).to be true
        expect(remove_task.completed?).to be true
      end
    end # it

    it "should execute a udf with string parameters successfully" do
      register_task = client.register_udf(udf_body_string, "udf_str.lua", Aerospike::Language::LUA)

      expect(register_task.wait_till_completed).to be true
      expect(register_task.completed?).to be true

      key = Support.gen_random_key

      client.put(key, Aerospike::Bin.new('bin', 'value'))

      expect(client.batch_exists([key])).to eq [true]

      # res = client.execute_udf(key, 'udf_str', 'testStr', ['a long and serious looking string'])
      # expect(res).to eq "a long and serious looking string"

      res = client.execute_udf(key, 'udf_str', 'testStr', [])
      expect(res).to eq nil

      res = client.execute_udf(key, 'udf_str', 'testStr', ['A'])
      expect(res).to eq "A"

    end # it


    it "should execute a udf successfully" do

      register_task = client.register_udf(udf_body_delete, "udf_delete.lua", Aerospike::Language::LUA)

      register_task.wait_till_completed
      expect(register_task.completed?).to be true

      key1 = Support.gen_random_key
      key2 = Support.gen_random_key
      key3 = Support.gen_random_key

      client.put(key1, Aerospike::Bin.new('bin', 'value'))
      client.put(key2, Aerospike::Bin.new('bin', 'value'))
      client.put(key3, Aerospike::Bin.new('bin', 'value'))

      expect(client.batch_exists([key1, key2, key3])).to eq [true, true, true]

      client.execute_udf(key1, 'udf_delete', 'delete_record')
      client.execute_udf(key2, 'udf_delete', 'delete_record')
      client.execute_udf(key3, 'udf_delete', 'delete_record')

      expect(client.batch_exists([key1, key2, key3])).to eq [false, false, false]

    end

    it "should create a record in udf with :send_key => true successfully" do

      register_task = client.register_udf(udf_body_send_key, "udf_body_send_key.lua", Aerospike::Language::LUA)

      register_task.wait_till_completed
      expect(register_task.completed?).to be true

      key1 = Support.gen_random_key(50, :set => 'test1')
      key2 = Support.gen_random_key(50, :set => 'test1')

      client.execute_udf(key1, 'udf_body_send_key', 'create_record', ['bin1', 1], :send_key => true)
      client.execute_udf(key2, 'udf_body_send_key', 'create_record', ['bin1', 2], :send_key => true)

      rs = client.scan_all('test', 'test1')

      rs.each do |rec|
        expect(rec.key.user_key).to eq key1.user_key if rec.key.digest == key1.digest
        expect(rec.key.user_key).to eq key2.user_key if rec.key.digest == key2.digest
      end

    end

    it "should execute a UDF on all records" do
      ns = 'test'
      set = Support.rand_string(10)
      div = 2

      number_of_records = 100
      number_of_records.times do |i|
        key = Support.gen_random_key(50, {:set => set})
        bin1 = Aerospike::Bin.new('bin1', i * div)
        bin2 = Aerospike::Bin.new('bin2', -1)
        client.put(key, [bin1, bin2])
      end

      register_task = client.register_udf(udf_body, "udf1.lua", Aerospike::Language::LUA)
      expect(register_task.wait_till_completed).to be true
      expect(register_task.completed?).to be true

      statement = Aerospike::Statement.new(ns, set)
      ex_task = client.execute_udf_on_query(statement, "udf1", "testFunc1", [div])
      expect(ex_task.wait_till_completed).to be true
      expect(ex_task.completed?).to be true

      # read all data and make sure it is consistent
      recordset = client.scan_all(ns, set)
      cnt = 0
      recordset.each do |rec|
        expect(rec.bins['bin2']).to eq (rec.bins['bin1'] / div)
        cnt += 1
      end
      expect(cnt).to eq number_of_records
    end # it

    it "should execute a UDF on only a range of records" do
      ns = 'test'
      set = Support.rand_string(10)

      div = 2

      number_of_records = 100
      number_of_records.times do |i|
        key = Support.gen_random_key(50, {:set => set})
        bin1 = Aerospike::Bin.new('bin1', i * div)
        bin2 = Aerospike::Bin.new('bin2', -1)
        client.put(key, [bin1, bin2])
      end

      register_task = client.register_udf(udf_body, "udf1.lua", Aerospike::Language::LUA)
      expect(register_task.wait_till_completed).to be true
      expect(register_task.completed?).to be true

      index_task = client.create_index(ns, set, "index_int_#{set}", 'bin1', :numeric)
      expect(index_task.wait_till_completed).to be true
      expect(index_task.completed?).to be true

      statement = Aerospike::Statement.new(ns, set)
      statement.filters << Aerospike::Filter.Range('bin1', 0, number_of_records / 2)
      ex_task = client.execute_udf_on_query(statement, "udf1", "testFunc1", [div])

      # wait until UDF is run on all records
      expect(ex_task.wait_till_completed).to be true
      expect(ex_task.completed?).to be true

      # read all data and make sure it is consistent
      recordset = client.scan_all(ns, set)

      cnt = 0
      recordset.each do |rec|
        if rec.bins['bin1'] <= number_of_records / 2
          expect(rec.bins['bin2']).to eq (rec.bins['bin1'] / div)
        else
          expect(rec.bins['bin2']).to eq(-1)
        end
        cnt += 1
      end
      expect(cnt).to eq number_of_records

    end # it

  end # describe

end # describe
