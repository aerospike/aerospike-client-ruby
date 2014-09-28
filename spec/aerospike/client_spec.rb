require "spec_helper"

require 'aerospike/host'
require 'aerospike/key'
require 'aerospike/bin'

describe Aerospike::Client do

  let(:client) do
    described_class.new("127.0.0.1", 3000)
  end

  after do
    client.close
  end

  describe "#initialize" do

    it "should connect to the cluster successfully" do

      expect(client.connected?).to be true

    end

    it "should have at least one node" do

      expect(client.nodes.length).to be >= 1

    end

    it "should have at least one name in node name list" do

      expect(client.node_names.length).to be >= 1

    end

  end

  describe "#put and #get" do

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

        expect(client.connected?).to be true

        record = client.get(key)
        expect(record.bins).to eq bin_map

      end

      it "should put a STRING and get it successfully" do

        key = Support.gen_random_key
        client.put(key, Aerospike::Bin.new('bin', 'value'))

        expect(client.connected?).to be true

        record = client.get(key)
        expect(record.bins['bin']).to eq 'value'

      end

      it "should put a NIL and get it successfully" do

        key = Support.gen_random_key
        client.put(key, [Aerospike::Bin.new('bin', nil),
                         Aerospike::Bin.new('bin1', 'auxilary')]
                   )

        expect(client.connected?).to be true

        record = client.get(key)
        expect(record.bins['bin']).to eq nil

      end

      it "should put an INTEGER and get it successfully" do

        key = Support.gen_random_key
        bin = Aerospike::Bin.new('bin', rand(2**63))
        client.put(key, bin)

        expect(client.connected?).to be true

        record = client.get(key)
        expect(record.bins['bin']).to eq bin.value

      end

      it "should put an ARRAY and get it successfully" do

        key = Support.gen_random_key
        bin = Aerospike::Bin.new('bin', [
                                   "string",
                                   rand(2**63),
                                   [1, nil, 'this'],
                                   ["embedded array", 1984, nil, {2 => 'string'}],
                                   nil,
                                   {'array' => ["another string", 17]},
        ])
        client.put(key, bin)

        expect(client.connected?).to be true

        record = client.get(key)
        expect(record.bins['bin']).to eq bin.value

      end

      it "should put a MAP and get it successfully" do

        key = Support.gen_random_key
        bin = Aerospike::Bin.new('bin', {
                                   "string" => nil,
                                   rand(2**63) => {2 => 11},
                                   [1, nil, 'this'] => {nil => "nihilism"},
                                   nil => ["embedded array", 1984, nil, {2 => 'string'}],
                                   {11 => [11, 'str']} => nil,
                                   {} => {'array' => ["another string", 17]},
        })
        client.put(key, bin)

        expect(client.connected?).to be true

        record = client.get(key)
        expect(record.bins['bin']).to eq bin.value

      end

    end

    it "should write a key successfully - and read its header again" do

      key = Support.gen_random_key
      client.put(key, Aerospike::Bin.new('bin', 'value'))

      expect(client.connected?).to be true

      record = client.get_header(key)
      expect(record.bins).to be nil
      expect(record.generation).to eq 1
      expect(record.expiration).to be > 0
    end

  end

  describe "#put and #delete" do

    it "should write a key successfully - and delete it" do

      key = Support.gen_random_key
      client.put(key, Aerospike::Bin.new('bin', 'value'))

      existed = client.delete(key)
      expect(existed).to be true

    end

    it "should return existed = false on non-existing keys" do

      key = Support.gen_random_key
      existed = client.delete(key)
      expect(existed).to be false

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
      expect(exists).to be false

      client.put(key, Aerospike::Bin.new('bin', 'value'))

      exists = client.exists(key)
      expect(exists).to be true

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

    it "should #put, #add" do

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

      # expect(rec.bins).to be nil
      expect(rec.generation).to eq 2
    end

  end

  context "Batch commands" do

    it "should successfully check existence of keys" do

      key1 = Support.gen_random_key
      key2 = Support.gen_random_key
      key3 = Support.gen_random_key

      client.put(key1, Aerospike::Bin.new('bin', 'value'))
      client.put(key3, Aerospike::Bin.new('bin', 'value'))

      exists = client.batch_exists([key1, key2, key3])

      expect(exists.length).to eq 3

      expect(exists[0]).to be true
      expect(exists[1]).to be false
      expect(exists[2]).to be true

    end

    it "should successfully get keys" do

      key1 = Support.gen_random_key
      key2 = Support.gen_random_key
      key3 = Support.gen_random_key

      client.put(key1, Aerospike::Bin.new('bin', 'value'))
      client.put(key3, Aerospike::Bin.new('bin', 'value'))

      records = client.batch_get([key1, key2, key3])

      expect(records.length).to eq 3

      expect(records[0].key).to eq key1
      expect(records[1]).to be nil
      expect(records[2].key).to eq key3

    end

  end

  describe "benchmarks" do

    it "benchmark #put #get" do
      bin = Aerospike::Bin.new('bin', 'value')
      key = Support.gen_random_key

      record = nil
      Benchmark.bm do |bm|
        # joining an array of strings
        bm.report do
          1000.times do
            client.put(key, bin)
            record = client.get(key)
          end
        end
      end

    end

  end

end
