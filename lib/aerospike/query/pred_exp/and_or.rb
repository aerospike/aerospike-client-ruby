# frozen_string_literal: true

module Aerospike
  class PredExp
    class AndOr < PredExp
      def initialize(op, nexp)
        @op = op
        @nexp = nexp
      end

      def estimate_size
        8
      end

      def write(buffer, offset)
        # write type
        buffer.write_int16(@op, offset)
        offset += 2

        # write length
        buffer.write_int32(2, offset)
        offset += 4

        # write predicate count
        buffer.write_int16(@nexp, offset)
        offset += 2

        offset
      end
    end
  end
end
