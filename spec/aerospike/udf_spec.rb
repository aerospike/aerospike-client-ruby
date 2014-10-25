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

    let(:udf_body) do
      "function testFunc1(rec, div, str)
         local ret = map                     -- Initialize the return value (a map)
         local x = rec['bin1']                 -- Get the value from record bin named 'bin1'
         rec['bin2'] = (x / div)               -- Set the value in record bin named 'bin2'
         aerospike:update(rec)                 -- Update the main record

         return str                              -- Return the Return value and/or status
      end"
    end

    let(:udf_body_delete) do
      @udf_body_delete = "function delete_record(rec)
         aerospike:remove(rec)                   -- Delete main record, Populate the return status
      end"
    end

    let(:client) do
      described_class.new("127.0.0.1", 3000)
    end

    after do
      client.close
    end

    describe "register" do

      it "should register UDFs, list them and and then successfully drop them" do

        (1..10).to_a.each do |i|
          register_task = client.register_udf(udf_body, "udf#{i}.lua", Aerospike::Language::LUA)

          expect(register_task.wait_till_completed).to be true
          expect(register_task.completed?).to be true
        end

        # should list the udfs
        udf_list = client.list_udf
        expect(udf_list.select { |item| item.filename =~ /udf(1|2|3)\.lua/ }.length).to eq 3

        (1..10).to_a.each do |i|
          remove_task = client.remove_udf("udf#{i}.lua")

          expect(remove_task.wait_till_completed).to be true
          expect(remove_task.completed?).to be true
        end

      end # it

      it "should execute a udf with parameters successfully" do
        STR = 'a long and serious looking string'
        register_task = client.register_udf(udf_body, "udf_params.lua", Aerospike::Language::LUA)

        expect(register_task.wait_till_completed).to be true
        expect(register_task.completed?).to be true

        key = Support.gen_random_key

        client.put(key, Aerospike::Bin.new('bin1', 120))
        client.put(key, Aerospike::Bin.new('bin', 'value'))

        expect(client.batch_exists([key])).to eq [true]

        res = client.execute_udf(key, 'udf_params', 'testFunc1', [1, STR])
        expect(res).to eq STR

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

    end # describe

  end # describe

end # describe
