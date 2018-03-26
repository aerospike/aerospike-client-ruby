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

RSpec.describe Aerospike::Connection::Authenticate do
  let(:command) { double }
  let(:conn) { double }
  let(:password) { 'password' }
  let(:user) { 'user' }

  before do
    allow(command).to receive(:authenticate)
    allow(conn).to receive(:close)
    allow(::Aerospike::AdminCommand).to receive(:new).and_return(command)
  end

  describe '::call' do
    subject(:authenticate) { described_class.call(conn, user, password) }

    context 'when authentication is successful' do
      it { is_expected.to eq true }
    end

    context 'when authentication fails' do
      before do
        allow(command).to receive(:authenticate).and_raise(::Aerospike::Exceptions::Aerospike.new(0))
      end

      it do
        expect { authenticate }.to raise_error(
          ::Aerospike::Exceptions::InvalidCredentials
        ) do |_|
          expect(conn).to have_received(:close)
        end
      end
    end
  end
end