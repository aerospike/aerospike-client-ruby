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

RESOURCE_PATH = Pathname.new(__dir__).join('..', '..', 'resources')

def resource(*path)
  RESOURCE_PATH.join(*path).to_s
end

describe Aerospike::Socket::SSL, skip: !Support.tls_supported? do
  let(:tls_options) { {} }

  describe '::build_ssl_context' do
    subject(:build_ssl_context) { described_class.build_ssl_context(tls_options) }

    context 'with pre-initialized context' do
      let(:ssl_context) { double }
      let(:tls_options) { { context:  ssl_context } }

      it { is_expected.to be ssl_context }
    end

    context 'without pre-initialized context' do
      let(:tls_options) { {} }

      before do
        allow(described_class).to receive(:create_context)
        build_ssl_context
      end

      it { expect(described_class).to have_received(:create_context) }
    end
  end

  describe '::create_context' do
    subject(:create_context) { described_class.create_context(tls_options) }

    it { is_expected.to be_a(OpenSSL::SSL::SSLContext) }

    context 'when cert_file and pkey_file options are given' do
      let(:cert) { resource('ssl', 'test.cert.pem') }
      let(:pkey) { resource('ssl', 'test.key.pem') }
      let(:tls_options) { { cert_file: cert, pkey_file: pkey } }

      before do
        allow_any_instance_of(OpenSSL::SSL::SSLContext).to receive(:add_certificate)
      end

      it { expect(create_context).to have_received(:add_certificate) }
    end

    context 'with ca_file option' do
      let(:ca_file) { resource('ssl', 'ca.cert.pem') }
      let(:tls_options) { { ca_file: ca_file } }

      it { expect(create_context.ca_file).to be ca_file }
    end

    context 'with ca_path option' do
      let(:ca_path) { resource('ssl') }
      let(:tls_options) { { ca_path: ca_path } }

      it { expect(create_context.ca_path).to be ca_path }
    end
  end
end