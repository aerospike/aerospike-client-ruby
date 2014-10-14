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

  describe "LDT operations" do

    describe "LargeStack operations" do

      let(:client) do
        described_class.new("127.0.0.1", 3000)
      end

      after do
        client.close
      end

      let(:lstack) do
        client.get_large_stack(Support.gen_random_key, 'bbb')
      end

      context "a large stack object" do

        it "should #push and #peek an element" do

          for i in 1..100
            lstack.push(i)

            expect(lstack.size).to eq i

            expect(lstack.peek(1)).to eq [i]
          end

        end # it

        it "should #scan all elements" do

          for i in 1..100
            lstack.push(i)

            expect(lstack.size).to eq i
          end

          expect(lstack.scan).to eq (1..100).to_a.reverse!

        end # it

        it "should get and set capacity" do

          for i in 1..10
            lstack.push(i)

            expect(lstack.size).to eq i
          end

          lstack.capacity = 99
          expect(lstack.capacity).to eq 99

        end # it

      end # describe

    end # describe

  end # describe

end # describe
