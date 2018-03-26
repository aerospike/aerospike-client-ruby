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

RSpec.describe Aerospike::Cluster::CreateConnection do
  let(:cluster) { spy }
  let(:host) { spy }

  describe '::call' do
    subject(:create_connection) { described_class.call(cluster, host) }

    before do
      allow(::Aerospike::Connection::Create).to receive(:call)
      allow(::Aerospike::Connection::Authenticate).to receive(:call)
      allow(cluster).to receive(:credentials_given?).and_return(authenticate)
    end

    context 'when user and password is given' do
      let(:authenticate) { true }

      before { create_connection }

      it { expect(::Aerospike::Connection::Authenticate).to have_received(:call) }
    end

    context 'when user and password is given' do
      let(:authenticate) { false }

      before { create_connection }

      it { expect(::Aerospike::Connection::Authenticate).not_to have_received(:call) }
    end
  end
end
