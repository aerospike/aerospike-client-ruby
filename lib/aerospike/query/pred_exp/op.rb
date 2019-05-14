# frozen_string_literal: true

module Aerospike
  class PredExp
    class Op < PredExp
      def initialize(op)
        @op = op
      end

      def estimate_size
        6
      end

      def write(buffer, offset)
        # write type
        buffer.write_int16(@op, offset)
        offset += 2

        # write zero length
        buffer.write_int32(0, offset)
        offset += 4

        offset
      end
    end
  end
end
