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

RSpec.describe Aerospike::Cluster::FindNode do
  let(:cluster) { double }
  let(:peers) { double }
  let(:node_name) { 'node' }

  describe '::call' do
    before do
      allow(cluster).to receive(:find_node_by_name).and_return(cluster_node)
      allow(peers).to receive(:find_node_by_name).and_return(peer_node)
    end

    subject(:find_node) { described_class.call(cluster, peers, node_name) }

    context 'when node is found in cluster' do
      let(:cluster_node) { spy }
      let(:peer_node) { spy }

      before { find_node }

      it { is_expected.to be cluster_node }
      it { expect(cluster_node).to have_received(:increase_reference_count!)  }
    end

    context 'when node is not found in cluster but in peers' do
      let(:cluster_node) { nil }
      let(:peer_node) { spy }

      before { find_node }

      it { is_expected.to be peer_node }
      it { expect(peer_node).to have_received(:increase_reference_count!)  }
    end

    context 'when node is not found' do
      let(:cluster_node) { nil }
      let(:peer_node) { nil }

      before { find_node }

      it { is_expected.to be_nil }
    end
  end
end
