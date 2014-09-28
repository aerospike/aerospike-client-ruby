require "spec_helper"

require "aerospike/policy/priority"

describe Aerospike::Policy do

  describe "#initialize" do

    it "should make a policy with default values" do

      policy = described_class.new

      expect(policy.class).to eq described_class
      expect(policy.Priority).to eq Aerospike::Priority::DEFAULT
      expect(policy.Timeout).to eq 0
      expect(policy.MaxRetries).to eq 2
      expect(policy.SleepBetweenRetries).to eq 0.5

    end
  end

end
