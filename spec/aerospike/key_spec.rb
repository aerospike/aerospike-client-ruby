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

describe Aerospike::Key do

  describe "#initialize" do

    it "should make a new key successfully" do

      k = described_class.new('namespace', 'set', 'string_value')

      expect(k.namespace).to eq 'namespace'
      expect(k.set_name).to eq 'set'
      expect(k.user_key).to eq 'string_value'

    end

  end # describe

  describe '#digest' do

    context 'with an integer user key' do

      it 'computes a correct digest' do

        k = described_class.new('namespace', 'set', 42)

        expect(k.digest.unpack('H*')).to eq ['386f89f493f3fd7ec333d43dd4dec8aa2e7d6ccf']

      end

    end # context

  end # describe

end # describe
