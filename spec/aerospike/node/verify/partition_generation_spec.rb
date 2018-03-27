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

RSpec.describe Aerospike::Node::Verify::PartitionGeneration do
  let(:node) { double }
  let(:partition_generation) { double }

  describe '#call' do
    subject(:call) { described_class.(node, info_map) }

    before do
      allow(node).to receive(:partition_generation).and_return(partition_generation)
      allow(partition_generation).to receive(:update).with(Integer)
      allow(partition_generation).to receive(:changed?)
    end

    context 'with empty info' do
      let(:info_map) { {} }

      it { expect { call }.to raise_error(Aerospike::Exceptions::Parse) }
    end

    context 'with valid info' do
      let(:info_map) { { 'partition-generation' => '2' } }

      before { call }

      it { expect(partition_generation).to have_received(:update).with(2) }
    end
  end
end
