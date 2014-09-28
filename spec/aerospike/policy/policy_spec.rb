require "spec_helper"

require "aerospike/policy/priority"

describe Aerospike::Policy do

  describe "#initialize" do

    it "should make a policy with default values" do

      policy = described_class.new

      expect(policy.class).to eq described_class
      expect(policy.priority).to eq Aerospike::Priority::DEFAULT
      expect(policy.timeout).to eq 0
      expect(policy.max_retries).to eq 2
      expect(policy.sleep_between_retries).to eq 0.5

    end
  end

end
