# frozen_string_literal: true

RSpec.describe Aerospike::Cluster::FindNodesToRemove do
  let(:cluster) { double }
  let(:node_found_in_partition_map) { true }
  let(:node) { double(::Aerospike::Node) }
  let(:healthy_node) { double(::Aerospike::Node) }

  before do
    allow(cluster).to receive(:nodes).and_return(nodes)
    allow(cluster).to receive(:find_node_in_partition_map).and_return(node_found_in_partition_map)

    allow(node).to receive(:active?).and_return(node_active)
    allow(node).to receive(:failed?).and_return(node_failed)
    allow(node).to receive(:referenced?).and_return(node_referenced)

    allow(healthy_node).to receive(:active?).and_return(true)
    allow(healthy_node).to receive(:failed?).and_return(false)
    allow(healthy_node).to receive(:referenced?).and_return(true)
  end

  describe '::call' do
    subject(:call) { described_class.call(cluster, refresh_count) }

    let(:refresh_count) { 0 }
    let(:node_active) { true }
    let(:node_referenced) { true }
    let(:node_failed) { false }

    context 'with refresh_count == 0' do
      let(:nodes) { [node] }
      let(:refresh_count) { 0 }

      before do
        call
      end

      it { expect(node).to have_received(:failed?).with(5) }
    end

    context 'with refresh_count > 0' do
      let(:nodes) { [node, healthy_node] }
      let(:refresh_count) { 1 }

      before do
        call
      end

      context 'with non-referenced node' do
        let(:node_referenced) { false }

        it { expect(node).to have_received(:failed?).with(no_args) }
      end
    end

    context 'with single-node cluster' do
      let(:nodes) { [node] }

      context 'when node is healty' do
        let(:node) { healthy_node }

        it { is_expected.to be_empty }
      end

      context 'when nodes is inactive' do
        let(:node_active) { false }

        it { is_expected.to include node }
      end

      context 'when node has failed' do
        let(:node_failed) { true }

        it { is_expected.to include node }
      end
    end

    context 'with multi-node cluster' do
      let(:nodes) { [node, healthy_node] }
      let(:refresh_count) { 1 }

      context 'with a non-referenced node, not found in partition map' do
        let(:node_referenced) { false }
        let(:node_found_in_partition_map) { false }

        it { is_expected.to include node }
      end

      context 'with a non-referenced node, found in partition map' do
        let(:node_referenced) { false }
        let(:node_found_in_partition_map) { true }

        it { is_expected.to be_empty }
      end

      context 'with a failed, non-referenced node' do
        let(:node_failed) { true }
        let(:node_referenced) { false }

        it { is_expected.to include node }
      end

      context 'with healty nodes' do
        let(:nodes) { [healthy_node, healthy_node] }

        it { is_expected.to be_empty }
      end
    end
  end
end
