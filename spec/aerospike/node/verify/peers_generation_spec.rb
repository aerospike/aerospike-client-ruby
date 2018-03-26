# frozen_string_literal: true

# Copyright 2018 Aerospike, Inc.
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

RSpec.describe Aerospike::Node::Verify::PeersGeneration do
  let(:node) { double }
  let(:generation) { double }
  let(:peers) { double }

  describe '#call' do
    subject(:call) { described_class.(node, info_map, peers) }

    let(:same_generation) { true }

    before do
      allow(node).to receive(:name).and_return('nodename')
      allow(node).to receive(:peers_generation).and_return(generation)
      allow(peers).to receive(:generation_changed=)
      allow(generation).to receive(:eql?).and_return(same_generation)
    end

    context 'with empty info' do
      let(:info_map) { {} }

      it { expect { call }.to raise_error(Aerospike::Exceptions::Parse) }
    end

    context 'with valid info' do
      let(:info_map) { { 'peers-generation' => '2' } }

      before { call }

      it { expect(peers).not_to have_received(:generation_changed=) }
      it { expect(generation).to have_received(:eql?).with(2) }

      context 'but new generation' do
        let(:same_generation) { false }

        it { expect(peers).to have_received(:generation_changed=).with(true) }
      end
    end
  end
end
