# fr# frozen_string_literal: true

module Aerospike
  class PredExp
    # Regex bit flags
    module RegexFlags
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
