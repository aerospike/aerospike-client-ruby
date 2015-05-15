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

describe Aerospike::Client do

  describe "LDT operations", :skip => true do

    describe "LargeMap operations" do

      let(:client) do
        described_class.new(Support.host, Support.port, :user => Support.user, :password => Support.password)
      end

      after do
        client.close
      end

      let(:lmap) do
        client.get_large_map(Support.gen_random_key, 'bbb')
      end

      context "a large map object" do

        it "should #put, #get and #remove an element" do

          for i in 1..100
            j = i + 10000
            lmap.put(i, j)

            expect(lmap.size).to eq 1

            expect(lmap.get(i)).to eq ({ i => j })
            lmap.remove(i)

            # expect(lmap.get(i)).to eq nil
          end

        end # it

        it "should #put_map and #scan all elements" do

          map = {}
          for i in 1..100
            map[i] = i
          end

          lmap.put_map(map)
          expect(lmap.size).to eq 100

          expect(lmap.scan).to eq map

        end # it

      end # describe

    end # describe

  end # describe

end # describe
