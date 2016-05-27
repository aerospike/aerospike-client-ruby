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
# require 'profile'

describe Aerospike::Client do

  describe "Scan operations" do

    let(:client) { Support.client }

    before :all do
      @namespace = "test"
      @set = "test998"
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

    def scan_method(type, bin_names=[], ops={})
      case type
      when :single_node
        client.nodes.map do |node|
          client.scan_node(node, @namespace, @set, bin_names, ops)
        end
      when :multiple_nodes
        [client.scan_all(@namespace, @set, bin_names, ops)]
      end
    end

    [:single_node, :multiple_nodes].each do |type|

      context "#{type.to_s}" do

        it "should return all records with all bins" do
          rs_list = scan_method(type, nil, :record_queue_size => 10)

          i = 0
          rs_list.each do |rs|
            rs.each do |rec|
              i +=1
              expect(rec.bins['bin1']).to eq "value#{rec.bins['bin2']}"
              expect(rec.bins.length).to eq 4

              # make sure the key was sent to the server
              expect(rec.key.user_key).to eq rec.bins['bin2']
            end
          end

          expect(i).to eq @record_count

        end # it

        it "should return all records with only bin1 and bin2" do
          rs_list = scan_method(type, ['bin1', 'bin2'], :record_queue_size => 10)

          i = 0
          rs_list.each do |rs|
            rs.each do |rec|
              i +=1
              expect(rec.bins['bin1']).to eq "value#{rec.bins['bin2']}"
              expect(rec.bins.length).to eq 2
            end
          end

          expect(i).to eq @record_count

        end # it

        it "should cancel without deadlock" do

          rs_list = scan_method(type, nil, :record_queue_size => 10)
          rs_list.each do |rs|
            sleep(1) # fill the queue to make sure deadlock doesn't happen
            rs.cancel
            expect {rs.next_record}.to raise_exception(Aerospike::ResultCode.message(Aerospike::ResultCode::SCAN_TERMINATED))
          end

          rs_list = scan_method(type)
          rs_list.each do |rs|
            rs = rs_list.first
            rs.cancel
            expect {rs.next_record}.to raise_exception(Aerospike::ResultCode.message(Aerospike::ResultCode::SCAN_TERMINATED))
          end

        end # it

        it "should cancel without deadlock inside each block" do

          rs_list = scan_method(type, nil, :record_queue_size => 10)
          rs_list.each do |rs|
            i = 0
            rs.each do |rec|
              i +=1
              break if (i == 15)
            end
            expect(i).to eq 15

            rs.cancel
            expect {rs.next_record}.to raise_exception(Aerospike::ResultCode.message(Aerospike::ResultCode::SCAN_TERMINATED))
          end

        end # it

      end # context

    end # do

  end

end # describe
