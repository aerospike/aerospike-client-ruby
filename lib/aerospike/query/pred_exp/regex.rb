# fr# frozen_string_literal: true

module Aerospike
  class PredExp
    class Regex < PredExp
      def initialize(op, flag = Flags::NONE)
        @op = op
        @flag = flag
      end

      def estimate_size
        10
      end

      def write(buffer, offset)
        # write op type
        buffer.write_int16(@op, offset)
        offset += 2

        # write length
        buffer.write_int32(4, offset)
        offset += 4

        # write predicate count
        buffer.write_int32(@flag, offset)
        offset += 4

        offset
      end

      # Regex bit flag
      module Flags
        # Regex defaults
        NONE = 0

        # Use POSIX Extended Regular Expression syntax when interpreting regex.
        EXTENDED = 1

        # Do not differentiate case.
        ICASE = 2

        # Do not report position of matches.
        NOSUB = 4

        # Match-any-character operators don't match a newline.
        NEWLINE = 8
      end
    end
  end
end
