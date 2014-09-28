require "spec_helper"

require "aerospike/policy/priority"
require "aerospike/policy/generation_policy"
require "aerospike/policy/record_exists_action"

describe Aerospike::WritePolicy do

  describe "#initialize" do

    it "should make a write policy with default values" do

      policy = described_class.new

      expect(policy.class).to eq described_class
      expect(policy.Priority).to eq Aerospike::Priority::DEFAULT
      expect(policy.Timeout).to eq 0
      expect(policy.MaxRetries).to eq 2
      expect(policy.SleepBetweenRetries).to eq 0.5

      expect(policy.RecordExistsAction).to eq Aerospike::RecordExistsAction::UPDATE
      expect(policy.GenerationPolicy).to eq Aerospike::GenerationPolicy::NONE
      expect(policy.Generation).to eq 0
      expect(policy.Expiration).to eq 0
      expect(policy.SendKey).to be true

    end
  end

end
