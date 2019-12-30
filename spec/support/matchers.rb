require 'rspec/expectations'

module RSpec
  module Matchers
    def raise_aerospike_error(expected_result_code)
      raise_error(Aerospike::Exceptions::Aerospike) { |error|
        expect(error.result_code).to eq expected_result_code
      }
    end
  end
end
