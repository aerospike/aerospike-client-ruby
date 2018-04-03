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

RSpec.describe Aerospike::Node::Verify::Name do
  let(:node) { double }

  describe '::call' do
    subject(:call) { described_class.(node, info_map) }

    let(:node_name) { 'thenode' }

    before do
      allow(node).to receive(:name).and_return(node_name)
      allow(node).to receive(:decrease_health)
      allow(node).to receive(:inactive!)
    end

    context 'when node name matches' do
      let(:info_map) { { 'node' => node_name } }

      before { call }

      it { expect(node).not_to have_received(:inactive!) }
      it { expect(node).not_to have_received(:decrease_health) }
    end

    context 'when node name does not match' do
      let(:info_map) { { 'node' => 'somethingelse' } }

      it do
        expect { call }.to raise_error(::Aerospike::Exceptions::Aerospike) do
          expect(node).to have_received(:inactive!)
        end
      end
    end

    context 'when node name is empty' do
      let(:info_map) { {} }

      it do
        expect { call }.to raise_error(::Aerospike::Exceptions::Aerospike) do
          expect(node).to have_received(:decrease_health)
        end
      end
    end
  end
end
