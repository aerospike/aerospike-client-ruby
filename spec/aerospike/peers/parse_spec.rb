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

RSpec.describe Aerospike::Peers::Parse do
  describe "::call" do
    subject(:parsed) { described_class.call(response) }

    let(:first_peer) { parsed.peers.first }

    context 'when empty response' do
      let(:response) { '1,,[]' }

      it { expect(parsed.generation).to eq 1 }
      it { expect(parsed.port_default).to be_nil }
      it { expect(parsed.peers).to be_empty }
    end

    context 'with tls names' do
      let(:response) do
        '3,3144,[[C1D4DC08D270008,aerospike,[192.168.33.10]],[C814DC08D270008,aerospike,[192.168.33.10:3244]]]'
      end

      it { expect(parsed.generation).to eq 3 }
      it { expect(parsed.port_default).to eq 3144 }
      it { expect(parsed.peers.size).to eq 2 }
      it { expect(first_peer.node_name).to eq 'C1D4DC08D270008' }
      it { expect(first_peer.tls_name).to eq 'aerospike' }
      it { expect(first_peer.hosts.size).to eq 1 }
    end

    context 'with IPv6' do
      let(:response) { '1,,[[name,tls,[[::1]]]]' }

      it { expect { parsed }.to raise_error(::Aerospike::Exceptions::Parse) }
    end

    context 'with invalid response' do
      let(:response) { ',,' }

      it { expect { parsed }.to raise_error(::Aerospike::Exceptions::Parse) }
    end

    context 'with multiple hosts' do
      let(:response) { '2,3100,[[C1DA60C88270008,,[192.168.50.4,10.0.2.15]]]' }

      it { expect(first_peer.hosts.size).to eq 2 }
      it { expect(parsed.port_default).to eq 3100 }
      it { expect(first_peer.hosts.first.port).to eq 3100 }

    end
  end
end
