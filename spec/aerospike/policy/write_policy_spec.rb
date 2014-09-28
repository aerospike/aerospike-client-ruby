require "spec_helper"

require "aerospike/policy/priority"
require "aerospike/policy/generation_policy"
require "aerospike/policy/record_exists_action"

describe Aerospike::WritePolicy do

  describe "#initialize" do

    it "should make a write policy with default values" do

      policy = described_class.new

      expect(policy.class).to eq described_class
      expect(policy.priority).to eq Aerospike::Priority::DEFAULT
      expect(policy.timeout).to eq 0
      expect(policy.max_retries).to eq 2
      expect(policy.sleep_between_retries).to eq 0.5

      expect(policy.record_exists_action).to eq Aerospike::RecordExistsAction::UPDATE
      expect(policy.generation_policy).to eq Aerospike::GenerationPolicy::NONE
      expect(policy.generation).to eq 0
      expect(policy.expiration).to eq 0
      expect(policy.send_key).to be true

    end
  end

end
