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

describe Aerospike::Socket::TCP do
  let(:instance) { double }
  let(:sockaddr) { double }
  let(:host) { 'thehost' }
  let(:port) { 3000 }
  let(:timeout) { 0.5 }

  describe '::connect' do
    subject(:connect) { described_class.connect(host, port, timeout) }

    before do
      allow(described_class).to receive(:new).and_return(instance)
      allow(::Socket).to receive(:sockaddr_in).and_return(sockaddr)
    end

    context 'when connection is successful' do
      before do
        allow(instance).to receive(:connect_nonblock)
        connect
      end

      it do
        expect(described_class).to have_received(:new).with(
          ::Socket::AF_INET, ::Socket::SOCK_STREAM, 0
        )
      end

      it { expect(instance).to have_received(:connect_nonblock).with(sockaddr) }
      it { is_expected.to be instance }
    end

    context 'when connection is in progress' do
      before do
        allow(instance).to receive(:connect_nonblock).and_raise(Errno::EINPROGRESS)
        allow(::IO).to receive(:select)
      end

      it do
        expect { connect }.to raise_error(::Aerospike::Exceptions::Connection) do
          expect(instance).to have_received(:connect_nonblock).twice
          expect(::IO).to have_received(:select).with(nil, [instance], nil, timeout)
        end
      end
    end
  end
end
