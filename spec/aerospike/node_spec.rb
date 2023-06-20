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

RSpec.describe Aerospike::Node do
  let(:cluster) { spy }
  let(:nv) { spy }
  let(:instance) { described_class.new(cluster, nv) }
  let(:peers) { double }

  describe '#create_min_connections' do
    subject(:connections) { instance.connections }
    before do
      allow(cluster).to receive(:min_connections_per_node).and_return(10)
      allow(connections).to receive(:length).and_return(current_number_of_connections)
      allow(connections).to receive(:create)
      allow(connections).to receive(:offer)
      allow(instance).to receive(:connections).and_return(connections)
    end

    context 'when current connections is less than minimum connections' do
      let(:current_number_of_connections) { 5 }

      it 'creates the expected number of minimum connections' do
        instance.fill_connection_pool_up_to(10)
        expect(connections).to have_received(:create).exactly(5).times
        expect(connections).to have_received(:offer).exactly(5).times
      end
    end

    context 'when current connections is equal to minimum connections' do
      let(:current_number_of_connections) { 10 }

      it 'does not create any additional connections' do
        expect(connections).not_to have_received(:create)
        expect(connections).not_to have_received(:offer)
      end
    end
  end


  describe '#failed!' do
    subject(:failed!) { instance.failed! }

    it { expect { failed! }.to change(instance, :failed?).from(false).to(true) }
  end

  describe '#reset_failures!' do
    subject(:reset_failures!) { instance.reset_failures! }

    context 'when node has failures' do
      before { instance.failed! }

      it { expect { reset_failures! }.to change(instance, :failed?).from(true).to(false) }
    end

    context 'when node has no failures' do
      it { expect { reset_failures! }.not_to change(instance, :failed?) }
    end
  end

  describe '#refresh_info' do
    subject(:refresh_info) { instance.refresh_info(peers) }

    before do
      allow(::Aerospike::Node::Refresh::Info).to receive(:call)
      refresh_info
    end

    it { expect(::Aerospike::Node::Refresh::Info).to have_received(:call).with(instance, peers) }
  end

  describe '#refresh_partitions' do
    subject(:refresh_partitions) { instance.refresh_partitions(peers) }

    before do
      allow(::Aerospike::Node::Refresh::Partitions).to receive(:call)
      refresh_partitions
    end

    it { expect(::Aerospike::Node::Refresh::Partitions).to have_received(:call).with(instance, peers) }
  end

  describe '#refresh_peers' do
    subject(:refresh_peers) { instance.refresh_peers(peers) }

    before do
      allow(::Aerospike::Node::Refresh::Peers).to receive(:call)
      refresh_peers
    end

    it { expect(::Aerospike::Node::Refresh::Peers).to have_received(:call).with(instance, peers) }
  end

  describe '#refresh_reset' do
    subject(:refresh_reset) { instance.refresh_reset }

    before do
      allow(::Aerospike::Node::Refresh::Reset).to receive(:call)
      refresh_reset
    end

    it { expect(::Aerospike::Node::Refresh::Reset).to have_received(:call).with(instance) }
  end
end
