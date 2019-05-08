# frozen_string_literal: true

module Aerospike
  class PredExp
    class StringValue < PredExp
      def initialize(value, type)
        @value = value
        @type = type
      end

      def estimate_size
        @value.bytesize + 6
      end

      def write(buffer, offset)
        buffer.write_int16(@type, offset)
        offset += 2

        len = buffer.write_binary(@value, offset + 4)
        buffer.write_int32(len, offset)
        offset += 4 + len

        offset
      end
    end
  end
end
