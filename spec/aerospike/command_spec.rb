# frozen_string_literal: true

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

RSpec.describe Aerospike::Command do
  context Aerospike::ExistsCommand do
    let(:cluster) { Support.client.cluster }
    let(:policy) { Support.client.default_read_policy }
    let(:key) { Support.gen_random_key }

    subject { described_class.new(cluster, policy, key) }

    describe '#execute' do
      it "can retry even if parse_result fails" do
        expect(policy.max_retries).to be > 0

        expect(subject).to receive(:parse_result).once do
          expect(subject).to receive(:parse_result).and_call_original

          raise Errno::ECONNRESET
        end

        subject.execute

        expect(subject.exists).to be false
      end
    end
  end
end
