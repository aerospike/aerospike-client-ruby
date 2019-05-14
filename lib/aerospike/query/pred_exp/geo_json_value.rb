# frozen_string_literal: true

module Aerospike
  class PredExp
    class GeoJsonValue < PredExp
      def initialize(value, type)
        @value = value
        @type = type
      end

      def estimate_size
        @value.bytesize + 9
      end

      def write(buffer, offset)
        # tag
        buffer.write_uint16(@type, offset)
        offset += 2

        # len
        buffer.write_uint32(@value.bytesize + 3, offset)
        offset += 4

        # flags

        buffer.write_byte(0, offset)
        offset += 1

        # ncells
        buffer.write_uint16(0, offset)
        offset += 2

        # value
        len = buffer.write_binary(@value, offset)
        offset += len

        offset
      end
    end
  end
end
