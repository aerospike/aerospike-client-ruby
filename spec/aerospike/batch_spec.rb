# Copyright 2014-2020 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require 'aerospike'

describe Aerospike::Client do

  let(:client) { Support.client }

  describe "#batch_exists" do
    shared_examples_for 'a batch_exists request' do
      let(:batch_policy) {
        Aerospike::BatchPolicy.new(use_batch_direct: use_batch_direct)
      }
      let(:existing_keys) { Array.new(3) { Support.gen_random_key } }
      let(:no_such_key) { Support.gen_random_key }
      let(:keys) { existing_keys }
      subject(:result) { client.batch_exists(keys, batch_policy) }

      before do
        existing_keys.each_with_index do |key, idx|
          client.put(key, {
            'idx' => idx,
            'key' => key.user_key,
            'rnd' => rand
          })
        end
      end

      context 'when checking existing records' do
        it 'returns one result per key' do
          expect(result.length).to be keys.length
        end

        it 'returns true for each existing key' do
          expect(result).to eql [true] * keys.length
        end
      end

      context 'when checking non-existent records' do
        let(:keys) { [no_such_key].concat(existing_keys) }

        it 'returns one result per key' do
          expect(result.length).to be keys.length
        end

        it 'returns false for the non-existent record' do
          expect(result).to eql [false].concat([true] * (keys.length - 1))
        end
      end
    end

    context 'using batch index protocol' do
      let(:use_batch_direct) { false }

      it_behaves_like 'a batch_exists request'
    end

    context 'using batch direct protocol', skip: Support.min_version?('4.4.0') do
      let(:use_batch_direct) { true }

      it_behaves_like 'a batch_exists request'
    end
  end

  describe "#batch_get" do
    shared_examples_for 'a batch_get request' do
      let(:batch_policy) {
        Aerospike::BatchPolicy.new(use_batch_direct: use_batch_direct)
      }
      let(:existing_keys) { Array.new(3) { Support.gen_random_key } }
      let(:no_such_key) { Support.gen_random_key }
      let(:keys) { existing_keys }
      let(:bins) { nil }
      subject(:result) { client.batch_get(keys, bins, batch_policy) }

      before do
        existing_keys.each_with_index do |key, idx|
          client.put(key, {
            'idx' => idx,
            'key' => key.user_key,
            'rnd' => rand
          })
        end
      end

      context 'when fetching existing records' do
        it 'returns one record per key' do
          expect(result.length).to be keys.length
        end

        it 'returns the records in the same order' do
          expect(result.map(&:key)).to eql keys
        end
      end

      context 'when fetching a non-existent record' do
        let(:keys) { [no_such_key].concat(existing_keys) }

        it 'returns one item per key' do
          expect(result.length).to be keys.length
        end

        it 'returns nil instead of the non-existent record' do
          expect(result.first).to be_nil
        end

        it 'returns the remaining records' do
          expect(result[2..-1].map(&:key)).to eql keys.slice(2..-1)
        end
      end

      context 'when no bins are specified' do
        let(:bins) { nil }

        it 'returns all the bins' do
          expect(result.first.bins.keys).to eql %w[idx key rnd]
        end
      end

      context 'when given a list of bin names' do
        let(:bins) { %w[ idx rnd ] }

        it 'returns only the specified bins' do
          expect(result.first.bins.keys).to eql %w[idx rnd]
        end
      end

      context 'when bin_names is :none' do
        let(:bins) { :none }

        it 'returns no bins' do
          expect(result.first.bins).to be_nil
        end
      end

      context 'when bin_names is :all' do
        let(:bins) { :all }

        it 'returns all the bins' do
          expect(result.first.bins.keys).to eql %w[idx key rnd]
        end
      end
    end

    context 'using batch index protocol' do
      let(:use_batch_direct) { false }

      it_behaves_like 'a batch_get request'
    end

    context 'using batch direct protocol', skip: Support.min_version?('4.4.0') do
      let(:use_batch_direct) { true }

      it_behaves_like 'a batch_get request'
    end
  end

  describe "#batch_get_header" do
    shared_examples_for 'a batch_get_header request' do
      let(:batch_policy) {
        Aerospike::BatchPolicy.new(use_batch_direct: use_batch_direct)
      }
      let(:existing_keys) { Array.new(3) { Support.gen_random_key } }
      let(:no_such_key) { Support.gen_random_key }
      let(:keys) { existing_keys }
      subject(:result) { client.batch_get_header(keys, batch_policy) }

      before do
        existing_keys.each_with_index do |key, idx|
          client.put(key, {
            'idx' => idx,
            'key' => key.user_key,
            'rnd' => rand
          }, {
            ttl: 1000
          })
        end
      end

      context 'when fetching existing records' do
        it 'returns one record per key' do
          expect(result.length).to be keys.length
        end

        it 'returns the records in the same order' do
          expect(result.map(&:key)).to eql keys
        end

        it 'returns the meta-data for each record' do
          expect(result.first.generation).to eq 1
          expect(result.first.ttl).to be_within(100).of(1000)
        end
      end

      context 'when fetching a non-existent record' do
        let(:keys) { [no_such_key].concat(existing_keys) }

        it 'returns one item per key' do
          expect(result.length).to be keys.length
        end

        it 'returns nil instead of the non-existent record' do
          expect(result.first).to be_nil
        end

        it 'returns the remaining records' do
          expect(result[2..-1].map(&:key)).to eql keys.slice(2..-1)
        end
      end
    end

    context 'using batch index protocol' do
      let(:use_batch_direct) { false }

      it_behaves_like 'a batch_get_header request'
    end

    context 'using batch direct protocol', skip: Support.min_version?('4.4.0') do
      let(:use_batch_direct) { true }

      it_behaves_like 'a batch_get_header request'
    end
  end
end
