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

describe Aerospike::Connection::Create do
  let(:host) { 'thehost' }
  let(:port) { 3000 }
  let(:tls_name) { nil }
  let(:timeout) { 1 }

  describe '::call' do
    before do
      allow(::Aerospike::Socket::SSL).to receive(:connect)
      allow(::Aerospike::Socket::TCP).to receive(:connect)
      described_class.call(
        host, port, timeout: timeout, tls_name: tls_name, ssl_options: ssl_options
      )
    end

    shared_examples_for 'a tcp socket' do
      it do
        expect(
          ::Aerospike::Socket::TCP
        ).to have_received(:connect).with(host, port, timeout)
      end
      it { expect(::Aerospike::Socket::SSL).not_to have_received(:connect) }
    end

    shared_examples_for 'a ssl socket' do
      it do
        expect(
          ::Aerospike::Socket::SSL
        ).to have_received(:connect).with(host, port, timeout, tls_name, ssl_options)
      end

      it { expect(::Aerospike::Socket::TCP).not_to have_received(:connect) }
    end

    context 'when ssl options indicates enabled' do
      let(:ssl_options) { { enable: true } }

      it_behaves_like 'a ssl socket'
    end

    context 'when ssl options are nil' do
      let(:ssl_options) { nil }

      it_behaves_like 'a tcp socket'
    end

    context 'when ssl options are empty' do
      let(:ssl_options) { {} }

      it_behaves_like 'a ssl socket'
    end
  end
end
