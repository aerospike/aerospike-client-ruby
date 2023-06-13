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

RSpec.describe Aerospike::Cluster do
  let(:policy) { spy(min_connections_per_node: 10, max_connections_per_node: 20) }
  let(:instance) { described_class.new(policy, hosts) }
  let(:hosts) { [] }

  describe '#create_node' do
    let(:nv) { double('nv') }
    let(:node) { instance_double(Aerospike::Node) }

    before do
      allow(Aerospike::Node).to receive(:new).with(instance, nv).and_return(node)
      allow(node).to receive(:connection_pool_init).with(policy)
    end

    it 'creates a new node and calls create_min_connections' do
      expect(Aerospike::Node).to receive(:new).with(instance, nv).and_return(node)
      expect(node).to receive(:connection_pool_init)
      new_node = instance.create_node(nv)
      expect(new_node).to eq(node)
    end

  end

  describe '#refresh_nodes' do
    subject(:refresh_nodes) { instance.refresh_nodes }
    let!(:peers) { Aerospike::Peers.new }
    let(:node) { spy }
    let(:node_generation_changed) { false }
    let(:generation_changed) { false }
    let(:nodes_to_remove) { [] }
    let(:peer_nodes) { {} }

    before do
      allow(Aerospike::Peers).to receive(:new).and_return(peers)
      allow(instance).to receive(:nodes).and_return(nodes)
      allow(instance).to receive(:add_nodes)
      allow(instance).to receive(:remove_nodes)
      allow(instance).to receive(:seed_nodes)
      allow(instance).to receive(:find_nodes_to_remove).and_return(nodes_to_remove)
      allow(node).to receive(:refresh_info)
      allow(node).to receive(:refresh_reset)
      allow(node).to receive(:refresh_peers)
      allow(node).to receive(:refresh_partitions)
      allow(node).to receive(:create_min_connection)
      allow(node.partition_generation).to receive(:changed?).and_return(node_generation_changed)
      allow(peers).to receive(:generation_changed?).and_return(generation_changed)
      allow(peers).to receive(:reset_refresh_count!)
      peers.nodes = peer_nodes

      refresh_nodes
    end

    context 'with no nodes' do
      let(:nodes) { [] }

      it { expect(instance).to have_received(:seed_nodes) }
      it { is_expected.to be true }
    end

    context 'with two nodes' do
      let(:nodes) { [node, node] }

      it { expect(node).to have_received(:refresh_reset).twice }

      context 'when peer generation has not changed' do
        let(:generation_changed) { false }

        it { expect(node).to have_received(:refresh_info).twice.with(peers) }
        it { expect(node).not_to have_received(:refresh_peers) }
        it { expect(node).not_to have_received(:refresh_partitions) }
        it { expect(instance).not_to have_received(:find_nodes_to_remove) }
      end

      context 'when peer generation has changed' do
        let(:generation_changed) { true }

        it { expect(node).to have_received(:refresh_info).twice.with(peers) }
        it { expect(node).to have_received(:refresh_peers).twice.with(peers) }
        it { expect(node).not_to have_received(:refresh_partitions) }
        it { expect(instance).to have_received(:find_nodes_to_remove).with(peers.refresh_count) }
        it { expect(peers).to have_received(:reset_refresh_count!) }
      end

      context 'with nodes to remove' do
        let(:generation_changed) { true }
        let(:nodes_to_remove) { [node] }

        it { expect(instance).to have_received(:remove_nodes).with(nodes_to_remove) }
        it { is_expected.to be true }
      end

      context 'with nodes to add' do
        let(:peer_nodes) { { 'node1' => node} }

        it { expect(instance).to have_received(:add_nodes).with(peer_nodes.values) }
        it { is_expected.to be true }
      end
    end
  end

  describe '#tls_enabled?' do
    subject { instance.tls_enabled? }

    before { allow(instance).to receive(:tls_options).and_return(tls_options) }

    context 'when tls_options enabled' do
      let(:tls_options) { { enable: true } }

      it { is_expected.to be true }
    end

    context 'when tls_options disabled' do
      let(:tls_options) { { enable: false } }

      it { is_expected.to be false }
    end

    context 'when tls_options is nil' do
      let(:tls_options) { nil }

      it { is_expected.to be false }
    end
  end
end
