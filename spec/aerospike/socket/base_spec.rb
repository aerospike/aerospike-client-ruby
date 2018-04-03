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

RSpec.describe Aerospike::Socket::Base do
  let(:instance) { ::Object.include(described_class).new }

  describe '::read_from_socket' do
    subject(:read_from_socket) { instance.read_from_socket(length) }

    let(:data) { 'bytes' }
    let(:length) { data.size }

    context 'when read is successful' do
      before do
        allow(instance).to receive(:read_nonblock).and_return(data)
        read_from_socket
      end

      it { expect(instance).to have_received(:read_nonblock) }
      it { is_expected.to be data }
    end

    context 'when read fails' do
      before { allow(instance).to receive(:read_nonblock).and_raise(::Errno::ECONNRESET) }

      it { expect { read_from_socket }.to raise_error(::Aerospike::Exceptions::Connection) }
    end
  end

  describe '::write_to_socket' do
    subject(:write_to_socket) { instance.write_to_socket(data) }

    let(:data) { 'bytes' }
    let(:written) { data.size }


    context 'when write is successful' do
      before do
        allow(instance).to receive(:write_nonblock).and_return(written)
        write_to_socket
      end

      it { expect(instance).to have_received(:write_nonblock) }
      it { is_expected.to be written }
    end

    context 'when write fails' do
      before { allow(instance).to receive(:write_nonblock).and_raise(::Errno::ECONNRESET) }

      it { expect { write_to_socket }.to raise_error(::Aerospike::Exceptions::Connection) }
    end
  end
end
