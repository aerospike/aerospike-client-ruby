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
  let(:host) { double() }
  let(:instance) { described_class.new(cluster, host) }
  let(:good_connection) { double(:connected? => true, :alive? => true) }
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
        connection = instance.poll()

        expect(connection).to be(good_connection)
      end
    end

    context "enforce max connections as a hard limit" do
      before do
        allow(cluster).to receive(:create_connection).with(host).and_return(good_connection)
        pool_size.times { instance.poll }
      end

      it "raise an max connection exceeded exception" do
        expect { instance.poll }.to raise_aerospike_error(-21)
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
        connection = instance.poll()

        expect(connection).to be(good_connection)
        expect(dead_connection).to have_received(:close)
      end
    end
  end
end
