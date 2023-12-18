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
      @set = "query100"
      @record_count = 100
      @record_count.times do |i|
        key = Aerospike::Key.new(@namespace, @set, i)
        bin_map = {
          'bin1' => "value#{i}",
          'bin2' => i,
          'bin3' => [ i, i + 1_000, i + 1_000_000 ],
          'bin4' => { "key#{i}" => i },
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
      it "returns all record bins" do
        stmt = Aerospike::Statement.new(@namespace, @set)
        stmt.filters << Aerospike::Filter.Equal('bin2', 1)
        rs = client.query(stmt)
        count = 0
        rs.each do |rec|
          count += 1
          expect(rec.bins.keys).to contain_exactly("bin1", "bin2", "bin3", "bin4")
        end
        expect(count).to be > 0
      end

      it "returns only the selected bins" do
        bins = ["bin1", "bin2"]
        stmt = Aerospike::Statement.new(@namespace, @set, bins)
        stmt.filters << Aerospike::Filter.Equal('bin2', 1)
        rs = client.query(stmt)
        count = 0
        rs.each do |rec|
          count += 1
          expect(rec.bins.keys).to contain_exactly("bin1", "bin2")
        end
        expect(count).to be > 0
      end

      it "returns only the record meta data", skip: !Support.min_version?("3.15") do
        stmt = Aerospike::Statement.new(@namespace, @set)
        stmt.filters << Aerospike::Filter.Equal('bin2', 1)
        rs = client.query(stmt, include_bin_data: false)
        count = 0
        rs.each do |rec|
          count += 1
          expect(rec.bins).to be_nil
          expect(rec.generation).to_not be_nil
        end
        expect(count).to be > 0
      end
    end

    context "No Filter == Scan" do

      it "should return all records" do
        rs = client.query(Aerospike::Statement.new(@namespace, @set))

        i = 0
        rs.each do |rec|
          i +=1
          expect(rec.bins['bin1']).to eq "value#{rec.bins['bin2']}"
        end

        expect(i).to eq @record_count

      end # it

      it "should return selected bins from all records" do
        stmt = Aerospike::Statement.new(@namespace, @set, %w[bin1 bin2])
        rs = client.query(stmt)

        records = 0
        rs.each do |rec|
          records += 1
          expect(rec.bins.keys).to contain_exactly("bin1", "bin2")
        end

        expect(records).to eq @record_count
      end # it

      it "does not raise a KEY_NOT_FOUND error when querying a set that does not exist" do
        stmt = Aerospike::Statement.new(@namespace, "SetDoesNotExist")
        rs = client.query(stmt)

        records = 0
        expect { rs.each { records += 1 } }.not_to raise_error()
        expect(records).to eql(0)
      end

      it "should return relevant records with records_per_second" do
        set = "query1000"
        record_count = 1000
        record_count.times do |i|
          key = Aerospike::Key.new(@namespace, set, i)
          bin_map = {
            'bin1' => "value#{i}",
            'bin2' => i,
            'bin3' => [ i, i + 1_000, i + 1_000_000 ],
            'bin4' => { "key#{i}" => i },
          }
          Support.client.put(key, bin_map)
        end

        stmt = Aerospike::Statement.new(@namespace, set, ['bin1', 'bin2'])
        rs = client.query(stmt, :records_per_second => (@record_count / 4).to_i)

        i = 0
        tm = Time.now
        rs.each do |rec|
          i += 1
          expect(rec.bins['bin1']).to eq "value#{rec.bins['bin2']}"
          expect(rec.bins.length).to eq 2
        end

        expect(i).to eq record_count
        expect((Time.now - tm).to_i).to be >= 0

      end # it

    end # context

    context "Equal Filter" do

      context "Numeric Bins" do

        it "should return relevant records" do
          stmt = Aerospike::Statement.new(@namespace, @set, ['bin1', 'bin2'])
          stmt.filters = [Aerospike::Filter.Equal('bin2', 1)]
          rs = client.query(stmt)

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

        it "should return relevant records" do
          stmt = Aerospike::Statement.new(@namespace, @set)
          stmt.filters = [Aerospike::Filter.Equal('bin1', 'value1')]
          rs = client.query(stmt)

          i = 0
          rs.each do |rec|
            i +=1
            expect(rec.bins['bin1']).to eq "value#{rec.bins['bin2']}"
          end

          expect(i).to eq 1

        end # it

      end # context

    end # context

    context "Contains Filter" do

      context "List Bins" do

        it "should return relevant records" do
          stmt = Aerospike::Statement.new(@namespace, @set, ['bin2', 'bin3'])
          stmt.filters = [Aerospike::Filter.Contains('bin3', 42, :list)]
          rs = client.query(stmt)

          i = 0
          rs.each do |rec|
            i += 1
            expect(rec.bins['bin3']).to eq [42, 1_042, 1_000_042]
          end
          expect(i).to eq 1
        end # it

      end # context

      context "Map Keys" do

        it "should return relevant records" do
          stmt = Aerospike::Statement.new(@namespace, @set, ['bin4'])
          stmt.filters = [Aerospike::Filter.Contains('bin4', 'key42', :mapkeys)]
          rs = client.query(stmt)

          i = 0
          rs.each do |rec|
            i += 1
            expect(rec.bins['bin4']).to eq({ "key42" => 42 })
          end
          expect(i).to eq 1
        end # it

      end # context

      context "Map Values" do

        it "should return relevant records" do
          stmt = Aerospike::Statement.new(@namespace, @set, ['bin4'])
          stmt.filters = [Aerospike::Filter.Contains('bin4', 42, :mapvalues)]
          rs = client.query(stmt)

          i = 0
          rs.each do |rec|
            i += 1
            expect(rec.bins['bin4']).to eq({ "key42" => 42 })
          end
          expect(i).to eq 1
        end # it

      end # context

    end # context

    context "Range Filter" do

      context "Numeric Bins" do

        it "should return relevant records" do
          stmt = Aerospike::Statement.new(@namespace, @set)
          stmt.filters = [Aerospike::Filter.Range('bin2', 10, 20)]
          rs = client.query(stmt)

          i = 0
          rs.each do |rec|
            i +=1
            expect(rec.bins['bin1']).to eq "value#{rec.bins['bin2']}"
          end

          expect(i).to eq 11

        end # it

      end # context

      context "List Bins" do

        it "should return relevant records" do
          stmt = Aerospike::Statement.new(@namespace, @set)
          stmt.filters = [Aerospike::Filter.Range('bin3', 10, 20, :list)]
          rs = client.query(stmt)

          i = 0
          rs.each do |rec|
            i +=1
            idx = rec.bins['bin2']
            expect(rec.bins['bin3']).to eq([idx, 1_000 + idx, 1_000_000 + idx])
          end

          expect(i).to eq 11

        end # it

      end # context

      context "Map Values" do

        it "should return relevant records" do
          stmt = Aerospike::Statement.new(@namespace, @set)
          stmt.filters = [Aerospike::Filter.Range('bin4', 10, 20, :mapvalues)]
          rs = client.query(stmt)

          i = 0
          rs.each do |rec|
            i +=1
            idx = rec.bins['bin2']
            expect(rec.bins['bin4']).to eq({ "key#{idx}" => idx })
          end

          expect(i).to eq 11

        end # it

      end # context

    end # context

    context "Geospatial Filter", skip: !Support.feature?(Aerospike::Features::GEO) do

      let(:lon){ 103.9114 }
      let(:lat){ 1.3083 }
      let(:radius){ 1000 }
      let(:point){ Aerospike::GeoJSON.new({type: "Point", coordinates: [lon, lat]}) }
      let(:point2){ Aerospike::GeoJSON.new({type: "Point", coordinates: [lon + 1, lat + 1]}) }
      let(:region){ Aerospike::GeoJSON.new({type: "Polygon", coordinates: [[[103.6055, 1.1587], [103.6055, 1.4707], [104.0884, 1.4707], [104.0884, 1.1587], [103.6055, 1.1587]]]}) }

      before(:all) do
        tasks = []
        tasks << Support.client.create_index(@namespace, "geo", "geo_index", "location", :geo2dsphere)
        tasks << Support.client.create_index(@namespace, "geo", "geo_list_index", "locations", :geo2dsphere, :list)
        tasks << Support.client.create_index(@namespace, "geo", "geo_map_index", "locations", :geo2dsphere, :mapvalues)
        tasks.each(&:wait_till_completed)
        expect(tasks.all?(&:completed?)).to be true
      end

      after(:all) do
        Support.client.drop_index(@namespace, "geo", "geo_index")
        Support.client.drop_index(@namespace, "geo", "geo_list_index")
        Support.client.drop_index(@namespace, "geo", "geo_map_index")
      end

      before(:each) do
        Support.delete_set(Support.client, @namespace, "geo")
      end

      it "should return a point within the given GeoJSON region" do
        key = Aerospike::Key.new(@namespace, "geo", "p1")
        client.put(key, "location" => point)

        stmt = Aerospike::Statement.new(key.namespace, key.set_name, ["location"])
        stmt.filters = [Aerospike::Filter.geoWithinGeoJSONRegion("location", region)]
        rs = client.query(stmt)

        results = []
        rs.each{|record| results << record }
        expect(results.map(&:key)).to eq [key]
      end # it

      it "should return a point within the given radius" do
        key = Aerospike::Key.new(@namespace, "geo", "p1")
        client.put(key, "location" => point)

        stmt = Aerospike::Statement.new(key.namespace, key.set_name, ["location"])
        stmt.filters = [Aerospike::Filter.geoWithinRadius("location", lon, lat, radius)]
        rs = client.query(stmt)

        results = []
        rs.each{|record| results << record }
        expect(results.map(&:key)).to eq [key]
      end # it

      it "should return a region which contains the given GeoJSON point" do
        key = Aerospike::Key.new(@namespace, "geo", "r1")
        client.put(key, "location" => region)

        stmt = Aerospike::Statement.new(key.namespace, key.set_name, ["location"])
        stmt.filters = [Aerospike::Filter.geoContainsGeoJSONPoint("location", point)]
        rs = client.query(stmt)

        results = []
        rs.each{|record| results << record }
        expect(results.map(&:key)).to eq [key]
      end # it

      it "should return a region which contains the given lon/lat coordinates" do
        key = Aerospike::Key.new(@namespace, "geo", "r1")
        client.put(key, "location" => region)

        stmt = Aerospike::Statement.new(key.namespace, key.set_name, ["location"])
        stmt.filters = [Aerospike::Filter.geoContainsPoint("location", lon, lat)]
        rs = client.query(stmt)

        results = []
        rs.each{|record| results << record }
        expect(results.map(&:key)).to eq [key]
      end # it

      it "should match points within a list of locations" do
        key = Aerospike::Key.new(@namespace, "geo", "l1")
        client.put(key, "locations" => [point, point2])

        stmt = Aerospike::Statement.new(key.namespace, key.set_name, ["locations"])
        stmt.filters = [Aerospike::Filter.geoWithinRadius("locations", lon, lat, radius, :list)]
        rs = client.query(stmt)

        results = []
        rs.each{|record| results << record }
        expect(results.map(&:key)).to eq [key]
      end # it

      it "should match points within a map of locations" do
        key = Aerospike::Key.new(@namespace, "geo", "l1")
        client.put(key, "locations" => { "current" => point, "last" => point2 })

        stmt = Aerospike::Statement.new(key.namespace, key.set_name, ["locations"])
        stmt.filters = [Aerospike::Filter.geoWithinRadius("locations", lon, lat, radius, :mapvalues)]
        rs = client.query(stmt)

        results = []
        rs.each{|record| results << record }
        expect(results.map(&:key)).to eq [key]
      end # it

    end # context

    context "With A Stream UDF Query" do

      it "should return relevant records from UDF without any arguments" do

        register_task = client.register_udf(udf_body, "udf_empty.lua", Aerospike::Language::LUA)

        expect(register_task.wait_till_completed).to be true
        expect(register_task.completed?).to be true

        stmt = Aerospike::Statement.new(@namespace, @set)
        stmt.filters = [Aerospike::Filter.Range('bin2', 10, 20)]
        stmt.set_aggregate_function('udf_empty', 'filter_records', [], true)

        rs = client.query(stmt)

        i = 0
        rs.each do |rec|
          i +=1
          res = rec.bins["SUCCESS"]
          expect(res['bin1']).to eq "value#{res['bin2']}"
        end

        expect(i).to eq 11

      end # it

      it "should return relevant records from UDF with arguments" do

        register_task = client.register_udf(udf_body, "udf_empty.lua", Aerospike::Language::LUA)

        expect(register_task.wait_till_completed).to be true
        expect(register_task.completed?).to be true

        stmt = Aerospike::Statement.new(@namespace, @set)
        stmt.filters = [Aerospike::Filter.Range('bin2', 10, 20)]

        filter_value = 15
        stmt.set_aggregate_function('udf_empty', 'filter_records_param', [filter_value], true)

        rs = client.query(stmt)

        i = 0
        rs.each do |rec|
          i +=1
          res = rec.bins["SUCCESS"]
          expect(res['bin1']).to eq "value#{res['bin2']}"
          expect(res['bin2']).to be > filter_value
        end

        expect(i).to eq 5

      end # it

    end # context

    context "Run queries in the background" do
      it "executes a put operation in the background" do
        @set = "ds_set"
        key = Aerospike::Key.new(@namespace, @set, 'ds_key')
        bin1 = Aerospike::Bin.new("bin1", "integer_value")
        client.put(key, bin1)

        stmt = Aerospike::Statement.new(@namespace, @set)
        bin2 = Aerospike::Bin.new("bin2", "string_value")
        ops = [
          Aerospike::Operation.put(bin2)
        ]
        execute_task = client.query_execute(stmt, ops)
        execute_task.wait_till_completed

        expect(execute_task.completed?).to be true
        rec_set = client.scan_all(@namespace, @set)
        rec_set.each do |rec|
          expect(rec.bins['bin2']).to eq 'string_value'
        end

      end
    end

  end # describe

end # describe
