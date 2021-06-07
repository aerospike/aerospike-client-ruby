# Copyright 2014-2020 Aerospike, Inc.
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

require 'aerospike'
require 'benchmark'

describe Aerospike::Client do

  let(:client) { Support.client }

  [true, false].each do |rack_aware|
  [0, 1].each do |rack_id|
  [Aerospike::Replica::MASTER, Aerospike::Replica::MASTER_PROLES, Aerospike::Replica::PREFER_RACK, Aerospike::Replica::SEQUENCE, Aerospike::Replica::RANDOM].each do |replica_policy|
  context "alternate #replica_policies: #{replica_policy} and rack_aware: #{rack_aware}" do

  before do
    client.cluster.rack_aware = rack_aware
    client.cluster.rack_id = rack_id
    client.default_read_policy.replica = replica_policy
    client.default_write_policy.replica = replica_policy
    client.default_batch_policy.replica = replica_policy

    client.cluster.tend
  end

  after do
    client.cluster.rack_aware = false
    client.cluster.rack_id = 0
    client.default_read_policy.replica = Aerospike::Replica::MASTER
    client.default_write_policy.replica = Aerospike::Replica::MASTER
    client.default_batch_policy.replica = Aerospike::Replica::MASTER
  end

  describe "#initialize" do

    context "seed hosts" do
      around(:each) do |example|
        begin
          as_hosts = ENV.delete("AEROSPIKE_HOSTS")
          example.call
        ensure
          ENV["AEROSPIKE_HOSTS"] = as_hosts
        end
      end

      def cluster_seeds(client)
        client.instance_variable_get(:@cluster).seeds
      end

      it "accepts a single Host" do
        host = Aerospike::Host.new("10.10.10.10", 3333)
        client = Aerospike::Client.new(host, connect: false)

        expect(cluster_seeds(client)).to eq [host]
      end

      it "accepts a list of Hosts" do
        host1 = Aerospike::Host.new("10.10.10.10", 3333)
        host2 = Aerospike::Host.new("10.10.10.11", 3333)
        client = Aerospike::Client.new([host1, host2], connect: false)

        expect(cluster_seeds(client)).to eq [host1, host2]
      end

      it "accepts a single hostname" do
        client = Aerospike::Client.new("10.10.10.10", connect: false)

        expect(cluster_seeds(client)).to eq [Aerospike::Host.new("10.10.10.10", 3000)]
      end

      it "accepts a single hostname and port" do
        client = Aerospike::Client.new("10.10.10.10:3333", connect: false)

        expect(cluster_seeds(client)).to eq [Aerospike::Host.new("10.10.10.10", 3333)]
      end

      it "accepts a list of hostnames" do
        client = Aerospike::Client.new("10.10.10.10:3333,10.10.10.11", connect: false)

        expect(cluster_seeds(client)).to eq [Aerospike::Host.new("10.10.10.10", 3333), Aerospike::Host.new("10.10.10.11", 3000)]
      end

      it "reads a list of hostnames from AEROSPIKE_HOSTS" do
        ENV["AEROSPIKE_HOSTS"] = "10.10.10.10:3333,10.10.10.11"
        client = Aerospike::Client.new(connect: false)

        expect(cluster_seeds(client)).to eq [Aerospike::Host.new("10.10.10.10", 3333), Aerospike::Host.new("10.10.10.11", 3000)]
      end

      it "defaults to localhost:3000" do
        ENV["AEROSPIKE_HOSTS"] = nil
        client = Aerospike::Client.new(connect: false)

        expect(cluster_seeds(client)).to eq [Aerospike::Host.new("localhost", 3000)]
      end
    end

    context "default policies" do
      it "sets a default read policy" do
        read_policy = Aerospike::Policy.new(timeout: 2222)
        policy = {
          policies: {
            read: read_policy
          }
        }
        client = Aerospike::Client.new(connect: false, policy: policy)

        expect(client.default_read_policy).to eq read_policy
      end

      it "creates a default read policy" do
        policy = {}
        client = Aerospike::Client.new(connect: false, policy: policy)

        expect(client.default_read_policy).to be_an Aerospike::Policy
      end

      it "sets a default write policy" do
        write_policy = Aerospike::WritePolicy.new(send_key: true)
        policy = {
          policies: {
            write: write_policy
          }
        }
        client = Aerospike::Client.new(connect: false, policy: policy)

        expect(client.default_write_policy).to eq write_policy
      end

      it "creates a default write policy" do
        policy = {}
        client = Aerospike::Client.new(connect: false, policy: policy)

        expect(client.default_write_policy).to be_an Aerospike::WritePolicy
      end

      it "sets a default batch policy" do
        batch_policy = Aerospike::BatchPolicy.new(send_key: true)
        policy = {
          policies: {
            batch: batch_policy
          }
        }
        client = Aerospike::Client.new(connect: false, policy: policy)

        expect(client.default_batch_policy).to eq batch_policy
      end

      it "creates a default batch policy" do
        policy = {}
        client = Aerospike::Client.new(connect: false, policy: policy)

        expect(client.default_batch_policy).to be_an Aerospike::BatchPolicy
      end

      it "sets a default query policy" do
        query_policy = Aerospike::QueryPolicy.new(send_key: true)
        policy = {
          policies: {
            query: query_policy
          }
        }
        client = Aerospike::Client.new(connect: false, policy: policy)

        expect(client.default_query_policy).to eq query_policy
      end

      it "creates a default query policy" do
        policy = {}
        client = Aerospike::Client.new(connect: false, policy: policy)

        expect(client.default_query_policy).to be_an Aerospike::QueryPolicy
      end

      it "sets a default scan policy" do
        scan_policy = Aerospike::ScanPolicy.new(send_key: true)
        policy = {
          policies: {
            scan: scan_policy
          }
        }
        client = Aerospike::Client.new(connect: false, policy: policy)

        expect(client.default_scan_policy).to eq scan_policy
      end

      it "creates a default scan policy" do
        policy = {}
        client = Aerospike::Client.new(connect: false, policy: policy)

        expect(client.default_scan_policy).to be_an Aerospike::ScanPolicy
      end
    end

  end

  describe "#connect" do
    subject(:client) { described_class.new(policy: client_policy, connect: false) }

    let(:client_policy) { Support.client_policy }

    shared_examples_for 'a cluster' do
      before do
        allow_any_instance_of(::Aerospike::Cluster).to receive(:supports_peers_protocol?).and_return(peers_enabled)
      end

      context 'with empty policy' do
        before do
          client.connect
        end

        it { is_expected.to be_connected }
        it { expect(client.nodes.size).to be >= 1 }
        it { expect(client.node_names.size).to be >= 1 }
      end

      context "with non-matching cluster name" do
        let(:client_policy) { Support.client_policy({ cluster_name: 'thisIsNotTheRealClusterName' }) }

        it { expect { client.connect }.to raise_error(Aerospike::Exceptions::Aerospike) }
      end
    end

    context 'when peers protocol is enabled' do
      let(:peers_enabled) { true }

      it_behaves_like 'a cluster'
    end

    context 'when peers protocol is disabled' do
      let(:peers_enabled) { false }

      it_behaves_like 'a cluster'
    end
  end

  describe "#put and #get" do

    context "bin names", skip: !Support.min_version?("4.2") do

      it "supports bin names with a max. length of 15 chars" do
        key = Support.gen_random_key
        bins = {
          'bin-name-len-15' => 'bin name with 15 chars'
        }

        client.put(key, bins)
        record = client.get(key)

        expect(record.bins).to eq bins
      end

      it "returns an error when bin name length exceeds 15 chars" do
        key = Support.gen_random_key
        bins = {
          'bin-name-size-16' => 'bin name with 16 chars'
        }

        expect {
          client.put(key, bins)
        }.to raise_error(Aerospike::Exceptions::Aerospike) { |error|
          error.result_code == Aerospike::ResultCode::BIN_NAME_TOO_LONG
        }
      end

    end

    context "data types" do

      it "should put a hash with bins and get it back successfully" do
        key = Support.gen_random_key
        bin_map = {
          'bin1' => 'value1',
          'bin2' => 2,
          'bin4' => ['value4', {'map1' => 'map val'}],
          'bin5' => {'value5' => [124, "string value"]},
        }
        client.put(key, bin_map)
        record = client.get(key)
        expect(record.bins).to eq bin_map
      end

      it "should put a hash with bins and get only some bins" do
        key = Support.gen_random_key
        bin_map = {
          'bin1' => 'value1',
          'bin2' => 2,
          'bin4' => ['value4', {'map1' => 'map val'}],
          'bin5' => {'value5' => [124, "string value"]},
        }
        client.put(key, bin_map)
        record = client.get(key, ['bin1', 'bin2'])
        expect(record.bins).to eq ({'bin1' => 'value1', 'bin2' => 2})
      end

      it "should put a STRING and get it successfully" do
        key = Support.gen_random_key
        client.put(key, Aerospike::Bin.new('bin', 'value'))
        record = client.get(key)
        expect(record.bins['bin']).to eq 'value'
      end

      it "should put a STRING with non-ASCII characters and get it successfully" do
        key = Support.gen_random_key
        client.put(key, Aerospike::Bin.new('bin', '柑橘類の葉'))
        record = client.get(key)
        expect(record.bins['bin']).to eq '柑橘類の葉'
      end

      it "should put a NIL and get it successfully" do
        key = Support.gen_random_key
        client.put(key, [Aerospike::Bin.new('bin', nil),
                         Aerospike::Bin.new('bin1', 'auxilary')]
                   )
        record = client.get(key)
        expect(record.bins['bin']).to eq nil
      end

      it "should put an INTEGER and get it successfully" do
        key = Support.gen_random_key
        bin = Aerospike::Bin.new('bin', rand(2**63))
        client.put(key, bin)
        record = client.get(key)
        expect(record.bins['bin']).to eq bin.value
      end

      it "should put a FLOAT and get it successfully" do
        key = Support.gen_random_key
        bin = Aerospike::Bin.new('bin', rand)
        client.put(key, bin)
        record = client.get(key)
        expect(record.bins['bin']).to eq bin.value
      end

      it "should put a BOOL and get it successfully" do
        key = Support.gen_random_key
        bin1 = Aerospike::Bin.new('bin1', true)
        bin2 = Aerospike::Bin.new('bin2', false)
        client.put(key, [bin1, bin2])
        record = client.get(key)
        expect(record.bins['bin1']).to eq bin1.value
        expect(record.bins['bin2']).to eq bin2.value
      end

      it "should put a GeoJSON value and get it successfully", skip: !Support.feature?(Aerospike::Features::GEO) do
        key = Support.gen_random_key
        bin = Aerospike::Bin.new('bin', Aerospike::GeoJSON.new({type: "Point", coordinates: [103.9114, 1.3083]}))
        client.put(key, bin)
        record = client.get(key)
        expect(record.bins['bin']).to eq bin.value
      end

      it "should put a LIST and get it successfully" do
        key = Support.gen_random_key
        value = [
          "string",
          rand(2**63),
          [1, nil, 'this'],
          ["embedded array", 1984, nil, {2 => 'string'}],
          nil,
          {'array' => ["another string", 17]},
        ]
        bin = Aerospike::Bin.new('bin', value)
        client.put(key, bin)
        record = client.get(key)
        expect(record.bins['bin']).to eq value
      end

      it "should put a MAP and get it successfully" do
        key = Support.gen_random_key
        value = {
          "string" => nil,
          rand(2**31) => {2 => 11},
          # [1, nil, 'this'] => {nil => "nihilism"},
          nil => ["embedded array", 1984, nil, {2 => 'string'}],
          # {11 => [11, 'str']} => nil,
          # {} => {'array' => ["another string", 17]},
        }
        bin = Aerospike::Bin.new('bin', value)
        client.put(key, bin)
        record = client.get(key)
        expect(record.bins['bin']).to eql value
      end

      it "should convert symbols to strings in MAP bin values" do
        key = Support.gen_random_key
        bin = Aerospike::Bin.new('map', { :foo => :bar })
        client.put(key, bin)
        record = client.get(key)
        expect(record.bins['map']).to eq({ 'foo' => 'bar' })
      end

      it "should convert symbols to strings in LIST bin values" do
        key = Support.gen_random_key
        bin = Aerospike::Bin.new('list', [ :foo, :bar ])
        client.put(key, bin)
        record = client.get(key)
        expect(record.bins['list']).to eq([ 'foo', 'bar' ])
      end

      it "should put a BYTE ARRAY and get it successfully" do
        key = Support.gen_random_key
        bytes = SecureRandom.random_bytes(1000)
        value = Aerospike::BytesValue.new(bytes)
        bin = Aerospike::Bin.new('bin', value)
        client.put(key, bin)
        record = client.get(key)
        expect(record.bins['bin']).to eq bytes
      end

      it "should put a LIST OF BYTE ARRAYS and get it successfully" do
        key = Support.gen_random_key
        bytes = Array.new(5) { SecureRandom.random_bytes(100) }
        values = bytes.map { |blob| Aerospike::BytesValue.new(blob) }
        bin = Aerospike::Bin.new('bin', values)
        client.put(key, bin)
        record = client.get(key)
        expect(record.bins['bin']).to eq bytes
      end
    end

    it "should raise an error if hash with non-string keys is passed as record" do
      key = Support.gen_random_key

      expect { client.put(key, {symbol_key: "string value"}) }.to raise_error(Aerospike::Exceptions::Aerospike)
    end

  end

  describe "#put and #delete" do

    it "should write a key successfully - and delete it" do
      key = Support.gen_random_key
      client.put(key, Aerospike::Bin.new('bin', 'value'))
      existed = client.delete(key)
      expect(existed).to eq true
    end

    it "should return existed = false on non-existing keys" do
      key = Support.gen_random_key
      existed = client.delete(key)
      expect(existed).to eq false
    end

    it "should apply durable delete policy", skip: !Support.min_version?("3.10") do
      key = Support.gen_random_key
      if Support.enterprise?
        expect { client.delete(key, durable_delete: true) }.to_not raise_exception
      else
        expect { client.delete(key, durable_delete: true) }.to raise_exception(/enterprise-only/i)
      end
    end

  end

  describe "#put and #touch" do

    it "should write a key successfully - and touch it to bump generation" do
      key = Support.gen_random_key
      client.put(key, Aerospike::Bin.new('bin', 'value'))

      client.touch(key)
      record = client.get_header(key)
      expect(record.generation).to eq 2
    end

  end

  describe "#put and #exists" do

    it "should write a key successfully - and check if it exists" do
      key = Support.gen_random_key

      exists = client.exists(key)
      expect(exists).to eq false

      client.put(key, Aerospike::Bin.new('bin', 'value'))

      exists = client.exists(key)
      expect(exists).to eq true
    end

  end

  describe "#put and change" do

    let(:key) do
      Support.gen_random_key
    end

    before do
      exists = client.exists(key)
      expect(exists).to eq false

      client.put(key, {'str' => 'value', 'int' => 10, 'float' => 3.14159})

      exists = client.exists(key)
      expect(exists).to eq true
    end

    it "should append to a key successfully" do
      client.append(key, {'str' => '1' })
      record = client.get(key)
      expect(record.bins['str']).to eq 'value1'
    end

    it "should prepend to a key successfully" do
      client.prepend(key, {'str' => '0' })
      record = client.get(key)
      expect(record.bins['str']).to eq '0value'
    end

    it "should add to an int key successfully" do
      client.add(key, {'int' => 10 })
      record = client.get(key)
      expect(record.bins['int']).to eq 20

      client.add(key, {'int' => -10 })
      record = client.get(key)
      expect(record.bins['int']).to eq 10
    end

    it "should add to a float key successfully" do
      client.add(key, {'float' => 2.71828 })
      record = client.get(key)
      expect(record.bins['float']).to eq 5.85987

      client.add(key, {'float' => -3.14159 })
      record = client.get(key)
      expect(record.bins['float']).to eq 2.71828
    end

  end

  describe "#operate" do

    let(:key) do
      Support.gen_random_key
    end

    let(:bin_str) do
      Aerospike::Bin.new('bin name', 'string')
    end

    let(:bin_int) do
      Aerospike::Bin.new('bin name', rand(456123890))
    end

    let(:bin_float) do
      Aerospike::Bin.new('bin name', rand)
    end

    it "should #add, #get" do
      client.operate(key, [
                       Aerospike::Operation.add(bin_int),
      ])
      rec = client.get(key)
      expect(rec.bins[bin_str.name]).to eq bin_int.value * 1
      expect(rec.generation).to eq 1
    end

    it "should #put, #append" do
      rec = client.operate(key, [
                             Aerospike::Operation.put(bin_str),
                             Aerospike::Operation.get,
      ])

      expect(rec.bins[bin_str.name]).to eq bin_str.value
      expect(rec.generation).to eq 1

      rec = client.operate(key, [
                             Aerospike::Operation.append(bin_str),
                             Aerospike::Operation.get,
      ])

      expect(rec.bins[bin_str.name]).to eq bin_str.value * 2
      expect(rec.generation).to eq 2
    end

    it "should #put, #prepend" do
      rec = client.operate(key, [
                             Aerospike::Operation.put(bin_str),
                             Aerospike::Operation.get,
      ])

      expect(rec.bins[bin_str.name]).to eq bin_str.value
      expect(rec.generation).to eq 1

      rec = client.operate(key, [
                             Aerospike::Operation.prepend(bin_str),
                             Aerospike::Operation.get,
      ])

      expect(rec.bins[bin_str.name]).to eq bin_str.value * 2
      expect(rec.generation).to eq 2
    end

    it "should #put, #add integer" do
      rec = client.operate(key, [
                             Aerospike::Operation.put(bin_int),
                             Aerospike::Operation.get,
      ])

      expect(rec.bins[bin_int.name]).to eq bin_int.value
      expect(rec.generation).to eq 1

      rec = client.operate(key, [
                             Aerospike::Operation.add(bin_int),
                             Aerospike::Operation.get,
      ])

      expect(rec.bins[bin_str.name]).to eq bin_int.value * 2
      expect(rec.generation).to eq 2
    end

    it "should #put, #add float" do
      rec = client.operate(key, [
                             Aerospike::Operation.put(bin_float),
                             Aerospike::Operation.get,
      ])

      expect(rec.bins[bin_float.name]).to eq bin_float.value
      expect(rec.generation).to eq 1

      rec = client.operate(key, [
                             Aerospike::Operation.add(bin_float),
                             Aerospike::Operation.get,
      ])

      expect(rec.bins[bin_str.name]).to be_within(0.00001).of(bin_float.value * 2)
      expect(rec.generation).to eq 2
    end

    it "should #put, #touch" do
      rec = client.operate(key, [
                             Aerospike::Operation.put(bin_int),
                             Aerospike::Operation.get,
      ])

      expect(rec.bins[bin_int.name]).to eq bin_int.value
      expect(rec.generation).to eq 1

      rec = client.operate(key, [
                             Aerospike::Operation.touch,
                             Aerospike::Operation.get_header,
      ])

      expect(rec.generation).to eq 2
    end

    it "should #put, #delete" do
      rec = client.operate(key, [
                             Aerospike::Operation.put(bin_int),
                             Aerospike::Operation.get,
      ])

      expect(rec.bins[bin_int.name]).to eq bin_int.value
      expect(rec.generation).to eq 1

      client.operate(key, [
                             Aerospike::Operation.delete,
      ])

      rec = client.operate(key, [
                             Aerospike::Operation.get,
      ])

      expect(rec).to be_nil
    end

    context "with multiple read operations on same bin" do

      let(:map_bin) { Aerospike::Bin.new("map", { "a" => 3, "b" => 2, "c" => 1 }) }

      context "with record bin multiplicity SINGLE" do

        it "returns the result of the last operation" do
          client.put(key, [map_bin])

          return_type = Aerospike::CDT::MapReturnType::KEY_VALUE
          ops = [
            Aerospike::CDT::MapOperation.get_index(map_bin.name, 0, return_type: return_type),
            Aerospike::CDT::MapOperation.get_by_rank(map_bin.name, 0, return_type: return_type)
          ]
          policy = Aerospike::OperatePolicy.new(record_bin_multiplicity: Aerospike::RecordBinMultiplicity::SINGLE)
          result = client.operate(key, ops, policy)

          value = result.bins[map_bin.name]
          expect(value).to eq({ "c" => 1 })
        end

      end

      context "with record bin multiplicity ARRAY" do

        it "returns the results of all operations as an array" do
          client.put(key, [map_bin])

          return_type = Aerospike::CDT::MapReturnType::KEY_VALUE
          ops = [
            Aerospike::CDT::MapOperation.get_index(map_bin.name, 0, return_type: return_type),
            Aerospike::CDT::MapOperation.get_by_rank(map_bin.name, 0, return_type: return_type)
          ]
          policy = Aerospike::OperatePolicy.new(record_bin_multiplicity: Aerospike::RecordBinMultiplicity::ARRAY)
          result = client.operate(key, ops, policy)

          value = result.bins[map_bin.name]
          expect(value).to eq([ { "a" => 3 }, { "c" => 1 } ])
        end

      end

    end

  end

  describe "#truncate" do
    it "deletes all records in the set" do
      records_before = 20
      namespace = 'test'
      set_name = 'test'
      records_before.times do
        client.put(Support.gen_random_key(20, set: set_name), {"i" => 42})
      end
      sleep(0.1)

      client.truncate(namespace, set_name)
      sleep(0.2) # give truncate some time to finish

      count = client.scan_all(namespace, set_name, nil).to_enum(:each).count
      expect(count).to eq(0)
    end

    it "deletes all records older than given timestamp" do
      records_before = 20
      records_after = 2
      namespace = 'test'
      set_name = 'test'
      records_before.times do
        client.put(Support.gen_random_key(20, set: set_name), {"i" => 42})
      end
      sleep(0.1)
      truncate_time = Time.now
      sleep(0.1)
      records_after.times do
        client.put(Support.gen_random_key(20, set: set_name), {"i" => 42})
      end
      sleep(0.1)

      client.truncate(namespace, set_name, truncate_time)
      sleep(0.2) # give truncate some time to finish

      count = client.scan_all(namespace, set_name, nil).to_enum(:each).count
      expect(count).to eq(records_after)
    end

    context "no last-update-timestamp specified" do
      before do
        allow(client).to receive(:supports_feature?)
          .with(Aerospike::Features::LUT_NOW)
          .and_return(lut_now)
      end

      context "server requires lut=now", skip: Support.min_version?('4.5.0.1') do
        let(:lut_now) { true }

        it "sends lut=now" do
          expect(client).to receive(:send_info_command)
            .with(kind_of(Aerospike::Policy), /^truncate:namespace=foo;set=bar;lut=now$/, kind_of(Aerospike::Node))
            .and_return("OK")

          client.truncate("foo", "bar")
        end
      end

      context "server does not support lut=now" do
        let(:lut_now) { false }

        it "does not send lut argument" do
          expect(client).to receive(:send_info_command)
            .with(kind_of(Aerospike::Policy), /^truncate:namespace=foo;set=bar$/, kind_of(Aerospike::Node))
            .and_return("OK")

          client.truncate("foo", "bar")
        end
      end

    end
  end

  describe "benchmarks", skip: true do

    it "benchmark #put #get" do
      bin = Aerospike::Bin.new('bin', 'value')
      key = Support.gen_random_key

      times = Benchmark.measure do
        1000.times do
          client.put(key, bin)
          client.get(key)
        end
      end
      puts times
    end

  end

  end # context for alternate replicas
  end # context for alternate rack_id values
  end # context for alternate rack_aware values
  end # each
end
