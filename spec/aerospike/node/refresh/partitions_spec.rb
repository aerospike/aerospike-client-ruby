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

RSpec.describe Aerospike::Node::Refresh::Partitions do
  let(:node) { double }
  let(:cluster) { double }
  let(:conn) { double}
  let(:peers) { double }
  let(:refresh_count) { 10 }
  let(:healthy) { true }

  before do
    allow(node).to receive(:tend_connection).and_return(conn)
    allow(node).to receive(:cluster).and_return(cluster)
    allow(cluster).to receive(:update_partitions)
    allow(conn).to receive(:close)
    allow(::Aerospike::Node::Refresh::Failed).to receive(:call)
  end

  describe '::call' do
    subject(:refresh) { described_class.call(node, peers) }

    before do
      allow(described_class).to receive(:tokenizer)
      allow(described_class).to receive(:should_refresh?).and_return(healthy)
    end

    context 'with healty node' do
      before { refresh }

      it { expect(cluster).to have_received(:update_partitions) }
    end

    context 'with failed node' do
      let(:healthy) { false }

      before { refresh }

      it { expect(cluster).not_to have_received(:update_partitions) }
    end

    context 'when cluster.update_partitions fails' do
      before do
        allow(cluster).to receive(:update_partitions).and_raise(::Aerospike::Exceptions::Aerospike.new(0))

        refresh
      end

      it { expect(conn).to have_received(:close) }
      it { expect(::Aerospike::Node::Refresh::Failed).to have_received(:call) }
    end
  end
end
