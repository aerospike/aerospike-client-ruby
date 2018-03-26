# frozen_string_literal: true

# Copyright 2018 Aerospike, Inc.
#
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

RSpec.describe Aerospike::Node::Generation do
  let(:instance) { described_class.new(0) }

  describe '#reset_changed!' do
    subject(:reset_changed) { instance.reset_changed! }

    context 'with updated value' do
      before { instance.update(1) }

      it { expect { reset_changed }.to change(instance, :changed?).from(true).to(false) }
    end

    context 'with same value' do
      before { instance.update(0) }

      it { expect { reset_changed }.not_to change(instance, :changed?) }
    end
  end

  describe '#update' do
    subject(:update) { instance.update(new_number) }

    context 'when setting new value' do
      let(:new_number) { 1 }

      it { expect { update }.to change(instance.number, :value).from(0).to(new_number) }

      it { expect { update }.to change(instance, :changed?).from(false).to(true) }
    end
  end
end
