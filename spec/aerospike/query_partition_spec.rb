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

describe Aerospike::Client do

  let(:client) { Support.client }

  let(:udf_body) do
    " local function map_record(record)
        -- Add name and age to returned map.
        -- Could add other record bins here as well.
        return map {bin1=record.bin1, bin2=record['bin2']}
      end

      function filter_records(stream)

        local function filter_name(record)
          return true
        end

        return stream : filter(filter_name) : map(map_record)
      end

      function filter_records_param(stream, value)

        local function filter_name(record)
          return record['bin2'] > value
        end

        return stream : filter(filter_name) : map(map_record)
      end"
  end

  describe "Query operations" do

    before :all do
      @namespace = "test"
      @set = "query1000"
      @record_count = 1000
      @record_count.times do |i|
        key = Aerospike::Key.new(@namespace, @set, i)
        bin_map = {
          'bin1' => "value#{i}",
          'bin2' => i,
          'bin3' => [i, i + 1_000, i + 1_000_000],
          'bin4' => { "key#{i}" => i }
        }
        Support.client.put(key, bin_map)
      end

      Support.client.drop_index(@namespace, @set, "index_str_bin1")
      Support.client.drop_index(@namespace, @set, "index_int_bin2")
      Support.client.drop_index(@namespace, @set, "index_lst_bin3")
      Support.client.drop_index(@namespace, @set, "index_mapkey_bin4")
      Support.client.drop_index(@namespace, @set, "index_mapval_bin4")

      tasks = []
      tasks << Support.client.create_index(@namespace, @set, "index_str_bin1", "bin1", :string)
      tasks << Support.client.create_index(@namespace, @set, "index_int_bin2", "bin2", :numeric)
      tasks << Support.client.create_index(@namespace, @set, "index_lst_bin3", "bin3", :numeric, :list)
      tasks << Support.client.create_index(@namespace, @set, "index_mapkey_bin4", "bin4", :string, :mapkeys)
      tasks << Support.client.create_index(@namespace, @set, "index_mapval_bin4", "bin4", :numeric, :mapvalues)
      tasks.each(&:wait_till_completed)
      expect(tasks.all?(&:completed?)).to be true
    end

    context "Bin selection" do
      it "returns all record bins for Equality" do
        $speed=nil
        stmt = Aerospike::Statement.new(@namespace, @set)
        stmt.filters << Aerospike::Filter.Equal('bin2', 1)
        rs = client.query_partitions(Aerospike::PartitionFilter.all, stmt)
        count = 0
        rs.each do |rec|
          puts "RESULT IS: #{rec.bins}"
          count += 1
          expect(rec.bins.keys).to contain_exactly("bin1", "bin2", "bin3", "bin4")
        end
        expect(count).to eq 1
      end

      it "returns all record bins for Range" do
        $speed=nil
        stmt = Aerospike::Statement.new(@namespace, @set)
        stmt.filters << Aerospike::Filter.Range('bin2', 1, 500)
        rs = client.query_partitions(Aerospike::PartitionFilter.all, stmt)
        count = 0
        rs.each do |rec|
          count += 1
          expect(rec.bins.keys).to contain_exactly("bin1", "bin2", "bin3", "bin4")
        end
        expect(count).to eq 500
      end

      it "returns all record bins for Range" do
        $speed=nil
        stmt = Aerospike::Statement.new(@namespace, @set)
        stmt.filters << Aerospike::Filter.Range('bin2', 1, 500)
        rs = client.query_partitions(Aerospike::PartitionFilter.by_range(0, 2047), stmt)
        count = 0
        rs.each do |rec|
          count += 1
          expect(rec.bins.keys).to contain_exactly("bin1", "bin2", "bin3", "bin4")
        end
        expect(count).to eq 229
      end

    end

  end # describe

end # describe
