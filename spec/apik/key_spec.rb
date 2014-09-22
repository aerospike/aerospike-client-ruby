require "spec_helper"

describe Apik::Key do

  describe "#initialize" do

    it "should make a new key successfully" do

      k = described_class.new('namespace', 'set', 'stringValue')

      expect(k.namespace).to eq 'namespace'
      expect(k.setName).to eq 'set'
      expect(k.userKey).to eq 'stringValue'

    end

  end # describe

end # describe
