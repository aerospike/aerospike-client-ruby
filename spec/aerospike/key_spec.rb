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

    def digest(key)
      described_class.new('namespace', 'set', key).digest.unpack('H*')[0]
    end

    context 'with an integer user key' do

      it 'computes a correct digest' do
        expect(digest(1)).to eq("82d7213b469812947c109a6d341e3b5b1dedec1f")
        expect(digest(0)).to eq("93d943aae37b017ad7e011b0c1d2e2143c2fb37d")
        expect(digest(-1)).to eq("22116d253745e29fc63fdf760b6e26f7e197e01d")
        expect(digest(-2**63)).to eq("7185c2a47fb02c996daed26b4e01b83240aee9d4")
        expect(digest(2**63-1)).to eq("1698328974afa62c8e069860c1516f780d63dbb8")
      end

      context 'v1.0.x compatibility' do
        it 'computes a backward compatible digest for all keys' do
          described_class.enable_v1_compatibility!
          k = described_class.new('namespace', 'set', 42)
          described_class.enable_v1_compatibility!(false)

          expect(k.digest.unpack('H*')).to eq ['2312f777f2a475512965d267110d9ae18c40f350']
        end

        it 'computes a backward compatible digest for specific keys' do
          k = described_class.new('namespace', 'set', 42, v1_compatible: true)

          expect(k.digest.unpack('H*')).to eq ['2312f777f2a475512965d267110d9ae18c40f350']
        end
      end

    end # context

    context 'with a string user key' do
      it 'computes a correct digest' do
        expect(digest('')).to eq("2819b1ff6e346a43b4f5f6b77a88bc3eaac22a83")
        expect(digest('s')).to eq("607cddba7cd111745ef0a3d783d57f0e83c8f311")
        expect(digest('a' * 10)).to eq("5979fb32a80da070ff356f7695455592272e36c2")
        expect(digest('m' * 100)).to eq("f00ad7dbcb4bd8122d9681bca49b8c2ffd4beeed")
        expect(digest('t' * 1000)).to eq("07ac412d4c33b8628ab147b8db244ce44ae527f8")
        expect(digest('-' * 10000)).to eq("b42e64afbfccb05912a609179228d9249ea1c1a0")
        expect(digest('+' * 100000)).to eq("0a3e888c20bb8958537ddd4ba835e4070bd51740")
      end
    end

    context 'with a byte array user key' do
      it 'computes a correct digest' do
        expect(digest(Aerospike::BytesValue.new(''))).to eq("327e2877b8815c7aeede0d5a8620d4ef8df4a4b4")
        expect(digest(Aerospike::BytesValue.new('s'))).to eq("ca2d96dc9a184d15a7fa2927565e844e9254e001")
        expect(digest(Aerospike::BytesValue.new('a' * 10))).to eq("d10982327b2b04c7360579f252e164a75f83cd99")
        expect(digest(Aerospike::BytesValue.new('m' * 100))).to eq("475786aa4ee664532a7d1ea69cb02e4695fcdeed")
        expect(digest(Aerospike::BytesValue.new('t' * 1000))).to eq("5a32b507518a49bf47fdaa3deca53803f5b2e8c3")
        expect(digest(Aerospike::BytesValue.new('-' * 10000))).to eq("ed65c63f7a1f8c6697eb3894b6409a95461fd982")
        expect(digest(Aerospike::BytesValue.new('+' * 100000))).to eq("fe19770c371774ba1a1532438d4851b8a773a9e6")
      end
    end

  end # describe

end # describe
