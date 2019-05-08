# fr# frozen_string_literal: true

module Aerospike
  class PredExp
    class Regex < PredExp
      def initialize(op, flags)
        @op = op
        @flags = flags
      end

      def estimate_size
        10
      end

      def write(buffer, offset)
        # write op type
        buffer.write_int16(op, offset)
        offset += 2

        # write length
        buffer.write_int32(4, offset)
        offset += 4

        # write predicate count
        buffer.write_int32(flags, offset)
        offset += 4

        offset
      end
    end
  end
end
