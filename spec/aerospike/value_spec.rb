# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Aerospike::Value do
  describe '::of' do
    subject(:of) { described_class.of(value) }

    shared_examples_for 'an invalid value' do
      it { expect { of }.to raise_error(Aerospike::Exceptions::Aerospike) }
    end

    shared_examples_for 'an IntegerValue' do
      it { is_expected.to be_a ::Aerospike::IntegerValue }
    end

    context 'when value is 2**63' do
      let(:value) { 2**63 }

      it_behaves_like 'an invalid value'
    end

    context 'when value is -2**63 - 1' do
      let(:value) { -2**63 - 1 }

      it_behaves_like 'an invalid value'
    end

    context 'when value is 2**63 - 1' do
      let(:value) { 2**63 - 1 }

      it_behaves_like 'an IntegerValue'
    end

    context 'when value is -2**63' do
      let(:value) { - 2**63 }

      it_behaves_like 'an IntegerValue'
    end
  end
end
