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

require "aerospike/policy/priority"

describe Aerospike::Policy do

  describe "#initialize" do

    it "should make a policy with default values" do

      policy = described_class.new

      expect(policy.class).to eq described_class
      expect(policy.priority).to eq Aerospike::Priority::DEFAULT
      expect(policy.timeout).to eq 0
      expect(policy.max_retries).to eq 2
      expect(policy.sleep_between_retries).to eq 0.5

    end
  end

end
