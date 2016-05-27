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

    describe "LargeList operations" do

      let(:client) { Support.client }

      let(:llist) do
        client.get_large_list(Support.gen_random_key, 'bbb')
      end

      context "a large list object" do

        it "should #add, #find and #remove an element" do

          for i in 1..100
            llist.add(i)

            expect(llist.size).to eq 1

            expect(llist.find(i)).to eq [i]
            llist.remove(i)

            expect(llist.find(i)).to eq []
          end

        end # it

        it "should #scan all elements" do

          for i in 1..100
            llist.add(i)

            expect(llist.size).to eq i
          end

          expect(llist.scan).to eq (1..100).to_a

        end # it

      end # describe

    end # describe

  end # describe

end # describe
