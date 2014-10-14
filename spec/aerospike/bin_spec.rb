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

require 'aerospike/value/value'

describe Aerospike::Bin do

  describe "#initialize" do

    it "should make a new bin successfully" do

      bin = described_class.new('bin', 'value')

      expect(bin.name).to eq 'bin'
      expect(bin.value).to eq 'value'

    end

  end # describe

  describe "#value=" do

    it "should use method to assign value" do

      bin = described_class.new('bin', nil)
      bin.value = 191

      expect(bin.name).to eq 'bin'
      expect(bin.value).to eq 191

    end

  end # describe

end # describe
