require "spec_helper"

describe Aerospike::Key do

  describe "#initialize" do

    it "should make a new key successfully" do

      k = described_class.new('namespace', 'set', 'string_value')

      expect(k.namespace).to eq 'namespace'
      expect(k.set_name).to eq 'set'
      expect(k.user_key).to eq 'string_value'

    end

  end # describe

end # describe
