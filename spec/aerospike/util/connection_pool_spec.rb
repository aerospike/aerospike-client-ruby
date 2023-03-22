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
require 'rspec'
require 'aerospike'
RSpec.describe Aerospike::ConnectionPool do
  let(:pool_size) { 5 }
  let(:cluster) { double(connection_queue_size: pool_size) }
  let(:host) { double }
  let(:instance) { described_class.new(cluster, host) }
  let(:good_connection) { double(:connected? => true, :alive? => true) }
  let(:connection_queue) { [] }
  let(:connection) { double('connection') }


  describe ".poll" do
    context "when pool is empty" do
      before do
        allow(cluster).to receive(:create_connection).with(host).and_return(good_connection)
      end

      it "creates a new connection" do
        connection = instance.poll
        expect(connection).to be(good_connection)
      end
    end

    context "when pool contains an idle connection" do
      before do
        instance << good_connection
      end

      it "returns the idle connection" do
        connection = instance.poll

        expect(connection).to be(good_connection)
      end
    end



    context "when pool contains a dead connection" do
      let(:dead_connection) { spy(:connected? => true, :alive? => false) }

      before do
        allow(dead_connection).to receive(:close)

        instance << dead_connection
        instance << good_connection
      end

      it "discards the dead connection" do
        connection = instance.poll

        expect(connection).to be(good_connection)
        expect(dead_connection).to have_received(:close)
      end
    end
  end


  describe 'enforce max_connection' do
    let(:connection_to_be_closed) { double('Aerospike::Connection', connected?: true, close: true) }

    it 'closes the connection and decrements total_connections count' do
      instance.total_connections = pool_size
      instance.cleanup(connection_to_be_closed)
      expect(connection_to_be_closed).to have_received(:close)
      expect(instance.total_connections).to eq(pool_size - 1)
    end

    before do
      allow(cluster).to receive(:create_connection).with(host).and_return(good_connection)
    end

    it 'creates a connection and increments total_connections count' do
      instance.total_connections = 0
      conn = instance.poll
      expect(conn).to be(good_connection)
      expect(instance.total_connections).to eq(1)
    end

    context "enforce max connections as a hard limit" do
      before do
        allow(cluster).to receive(:create_connection).with(host).and_return(good_connection)
        allow(instance).to receive(:cleanup)
        pool_size.times do
          conn = instance.poll
          connection_queue << conn
        end
      end

      it "raise an max connection exceeded exception" do
        expect { instance.poll }.to raise_aerospike_error(-21)
      end
    end

    context "create a new connection after close"
    before do
      allow(cluster).to receive(:create_connection).with(host).and_return(good_connection)
    end

    it 'creates maximum number of connections and closes one of them' do
      connection_queue << connection_to_be_closed
      pool = Aerospike::ConnectionPool.new(cluster, host)
      (1..pool_size).each do
        new_conn = pool.create
        connection_queue << new_conn
      end
      expect { pool.create }.to raise_aerospike_error(-21)
      connection_to_close = connection_queue[0]
      pool.cleanup(connection_to_close)
      expect(pool.total_connections).to eq(pool_size - 1)

      new_connection = pool.create
      expect(new_connection).to be(good_connection)
      expect(new_connection.connected?).to be true
      expect(pool.total_connections).to eq(pool_size)
    end
  end
end
