# frozen_string_literal: true

module Aerospike
  class PredExp
    class IntegerValue < PredExp
      def initialize(value, type)
        @value = value
        @type = type
      end

      def estimate_size
        14
      end

      def write(buffer, offset)
        # Write type
        buffer.write_int16(@type, offset)
        offset += 2

        # Write length
        buffer.write_int32(8, offset)
        offset += 4

        # Write value.
        buffer.write_int64(@value, offset)
        offset += 8

        offset
      end
    end
  end
end
