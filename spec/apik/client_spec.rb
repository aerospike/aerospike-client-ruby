require "spec_helper"

require 'apik/host'
require 'apik/key'
require 'apik/bin'

describe Apik::Client do

  let(:random_key) do
    Apik::Key.new('test', 'test', Support.rand_string(50))
  end

  let(:client) do
    described_class.new(nil, "127.0.0.1", 3000)
  end

  after do
    client.close
  end

  describe "#initialize" do

    it "should connect to the cluster successfully" do

      expect(client.connected?).to be true

    end

    it "should have at least one node" do

      expect(client.get_nodes.length).to be >= 1

    end

    it "should have at least one name in node name list" do

      expect(client.get_node_names.length).to be >= 1

    end

  end

  describe "#put and #get" do

    it "should write a key successfully - and read it again" do

      key = random_key
      client.put_bins(nil, key, Apik::Bin.new('bin', 'value'))

      expect(client.connected?).to be true

      record = client.get(nil, key)
      expect(record.bins['bin']).to eq 'value'

    end

    it "should write a key successfully - and read its header again" do

      key = random_key
      client.put_bins(nil, key, Apik::Bin.new('bin', 'value'))

      expect(client.connected?).to be true

      record = client.get_header(nil, key)
      expect(record.bins).to be nil
      expect(record.generation).to eq 1
      expect(record.expiration).to be > 0
    end

  end

  describe "#put and #delete" do

    it "should write a key successfully - and delete it" do

      key = random_key
      client.put_bins(nil, key, Apik::Bin.new('bin', 'value'))

      existed = client.delete(nil, key)
      expect(existed).to be true

    end

    it "should return existed = false on non-existing keys" do

      key = random_key
      existed = client.delete(nil, key)
      expect(existed).to be false

    end

  end

  describe "#put and #touch" do

    it "should write a key successfully - and touch it to bump generation" do

      key = random_key
      client.put_bins(nil, key, Apik::Bin.new('bin', 'value'))

      client.touch(nil, key)
      record = client.get_header(nil, key)
      expect(record.generation).to eq 2

    end

  end

  describe "#put and #exists" do

    it "should write a key successfully - and check if it exists" do

      key = random_key

      exists = client.exists(nil, key)
      expect(exists).to be false

      client.put_bins(nil, key, Apik::Bin.new('bin', 'value'))

      exists = client.exists(nil, key)
      expect(exists).to be true

    end

  end

  describe "benchmarks" do

    it "benchmark #put #get" do

      bin = Apik::Bin.new('bin', 'value')
      key = random_key

      Benchmark.bm do |bm|
        # joining an array of strings
        bm.report do
          1000.times do
            client.put_bins(nil, key, bin)
            record = client.get(nil, key)
          end
        end
      end

    end

  end

end
