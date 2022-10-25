# encoding: utf-8
# Copyright 2014-2022 Aerospike, Inc.
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

# require 'profile'

describe Aerospike::Client do

  describe "ScanPartition operations" do

    let(:client) { Support.client }

    before :all do
      @namespace = "test"
      @set = "scan1000"
      @record_count = 1000
      @record_count.times do |i|
        key = Aerospike::Key.new(@namespace, @set, i)
        bin_map = {
          'bin1' => "value#{i}",
          'bin2' => i,
          'bin4' => ['value4', {'map1' => 'map val'}],
          'bin5' => {'value5' => [124, "string value"]},
        }
        Support.client.put(key, bin_map, :send_key => true)
      end
    end

    def scan_method(filter, compressed, bin_names=[], ops={})
      ops[:use_compression] = compressed
      client.scan_partitions(filter, @namespace, @set, bin_names, ops)
    end

    [true, false].each do |compressed|
      it "should return all records with all bins" do
        rs = scan_method(Aerospike::PartitionFilter.all, compressed, nil, :record_queue_size => 10)

        i = 0
        rs.each do |rec|
          i +=1
          expect(rec.bins['bin1']).to eq "value#{rec.bins['bin2']}"
          expect(rec.bins.length).to eq 4

          # make sure the key was sent to the server
          expect(rec.key.user_key).to eq rec.bins['bin2']
        end

        expect(i).to eq @record_count

      end # it

      it "should return all records with all bins for a specific partition" do
        rs = scan_method(Aerospike::PartitionFilter.by_id(15), compressed, nil, :record_queue_size => 10)

        i = 0
        rs.each do |rec|
          i +=1
          expect(rec.bins['bin1']).to eq "value#{rec.bins['bin2']}"
          expect(rec.bins.length).to eq 4

          # make sure the key was sent to the server
          expect(rec.key.user_key).to eq rec.bins['bin2']
        end

        expect(i).to eq 1

      end # it

      it "should return all records with all bins with records_per_second" do
        rs = scan_method(Aerospike::PartitionFilter.all, compressed, nil, :record_queue_size => 10, :records_per_second => (@record_count/4).to_i)

        i = 0
        rs.each do |rec|
          i +=1
          expect(rec.bins['bin1']).to eq "value#{rec.bins['bin2']}"
          expect(rec.bins.length).to eq 4

          # make sure the key was sent to the server
          expect(rec.key.user_key).to eq rec.bins['bin2']
        end

        expect(i).to eq @record_count

      end # it

      it "should return only the selected bins" do
        rs = scan_method(Aerospike::PartitionFilter.all, compressed, ['bin1', 'bin2'], :record_queue_size => 10)

        count = 0
        rs.each do |rec|
          count += 1
          expect(rec.bins.keys).to contain_exactly('bin1', 'bin2')
          expect(rec.bins['bin1']).to eq "value#{rec.bins['bin2']}"
        end

        expect(count).to eq @record_count
      end

      it "should return only record meta data" do
        rs = scan_method(Aerospike::PartitionFilter.all, compressed, nil, include_bin_data: false)

        count = 0
        rs.each do |rec|
          count += 1
          expect(rec.bins).to be_nil
          expect(rec.generation).to_not be_nil
        end

        expect(count).to eq @record_count
      end

      it "should cancel without deadlock" do

        rs = scan_method(Aerospike::PartitionFilter.all, compressed, nil, :record_queue_size => 10)
        sleep(1) # fill the queue to make sure deadlock doesn't happen
        rs.cancel
        expect {rs.next_record}.to raise_exception(Aerospike::ResultCode.message(Aerospike::ResultCode::SCAN_TERMINATED))

        rs = scan_method(Aerospike::PartitionFilter.all, compressed)
        rs.cancel
        expect {rs.next_record}.to raise_exception(Aerospike::ResultCode.message(Aerospike::ResultCode::SCAN_TERMINATED))

      end # it

      it "should cancel without deadlock inside each block" do

        rs = scan_method(Aerospike::PartitionFilter.all, compressed, nil, :record_queue_size => 10)
        i = 0
        rs.each do |rec|
          i +=1
          break if (i == 15)
        end
        expect(i).to eq 15

        rs.cancel
        expect {rs.next_record}.to raise_exception(Aerospike::ResultCode.message(Aerospike::ResultCode::SCAN_TERMINATED))

      end # it

    end # do compressed

  end

end # describe
