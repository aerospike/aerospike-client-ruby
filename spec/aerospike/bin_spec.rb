require "spec_helper"

require 'aerospike/value/value'

describe Aerospike::Bin do

  describe "#initialize" do

    it "should make a new bin successfully" do

      bin = described_class.new('bin', 'value')

      expect(bin.name).to eq 'bin'
      expect(bin.value).to eq 'value'

    end

  end # describe

  describe "#value=" do

    it "should use method to assign value" do

      bin = described_class.new('bin', nil)
      bin.value = 191

      expect(bin.name).to eq 'bin'
      expect(bin.value).to eq 191

    end

  end # describe

end # describe
