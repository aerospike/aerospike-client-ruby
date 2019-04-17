# encoding: utf-8
# Copyright 2016 Aerospike, Inc.
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

module Aerospike
  describe Record do

    describe "#initialize" do

      context "expiration time" do

        it "converts an absolute expiration time to a relative ttl" do
          expiration = Time.now.to_i + 3600 - 1262304000
          record = Record.new(nil, nil, nil, nil, expiration)
          expect(record.ttl).to be_within(1).of(3600)
        end

        it "treats an expiration time of 0 as 'never expires'" do
          record = Record.new(nil, nil, nil, nil, 0)
          expect(record.ttl).to eq(TTL::NEVER_EXPIRE)
        end

        it "populates the :expiration attribute for backwards compatibility" do
          expiration = Time.now.to_i + 3600 - 1262304000
          record = Record.new(nil, nil, nil, nil, expiration)
          expect(record.expiration).to eq(record.ttl)
        end

      end

    end

  end
end
