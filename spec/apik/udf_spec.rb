require "spec_helper"

require 'apik/host'
require 'apik/key'
require 'apik/bin'
require 'apik/language'

describe Apik::Client do

  UDF_BODY = "function testFunc1(rec, div)
                 local ret = map                     -- Initialize the return value (a map)
                 local x = rec['bin1']                 -- Get the value from record bin named 'bin1'
                 rec['bin2'] = (x / div)               -- Set the value in record bin named 'bin2'
                 aerospike:update(rec)                 -- Update the main record
                 ret['status'] = 'OK'                   -- Populate the return status
                 return ret                             -- Return the Return value and/or status
              end
              "

  UDF_BODY = "function deleteRecord(rec)
                 aerospike:remove(rec)                   -- Delete main record, Populate the return status
              end"


  let(:client) do
    described_class.new(nil, "127.0.0.1", 3000)
  end

  after do
    client.close
  end

  describe "register" do

    it "should register UDFs, list them and and then successfully drop them" do

      [1, 2, 3].each do |i|
        register_task = client.register_udf(nil, UDF_BODY, "udf#{i}.lua", Apik::Language::LUA)

        register_task.wait_till_completed
        expect(register_task.completed?).to be true
      end

      # should list the udfs
      udf_list = client.list_udf
      expect(udf_list.select { |item| item.filename =~ /udf(1|2|3)\.lua/ }.length).to eq 3

      [1, 2, 3].each do |i|
        remove_task = client.remove_udf(nil, "udf#{i}.lua")

        remove_task.wait_till_completed
        expect(remove_task.completed?).to be true
      end
    end # it

  end # describe

end # describe
