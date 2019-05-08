# frozen_string_literal: true

module Aerospike
  class PredExp
    AND                = 1
    OR                 = 2
    NOT                = 3
    INTEGER_VALUE      = 10
    STRING_VALUE       = 11
    GEOJSON_VALUE      = 12
    INTEGER_BIN        = 100
    STRING_BIN         = 101
    GEOJSON_BIN        = 102
    LIST_BIN           = 103
    MAP_BIN            = 104
    INTEGER_VAR        = 120
    STRING_VAR         = 121
    GEOJSON_VAR        = 122
    RECSIZE            = 150
    LAST_UPDATE        = 151
    VOID_TIME          = 152
    INTEGER_EQUAL      = 200
    INTEGER_UNEQUAL    = 201
    INTEGER_GREATER    = 202
    INTEGER_GREATEREQ  = 203
    INTEGER_LESS       = 204
    INTEGER_LESSEQ     = 205
    STRING_EQUAL       = 210
    STRING_UNEQUAL     = 211
    STRING_REGEX       = 212
    GEOJSON_WITHIN     = 220
    GEOJSON_CONTAINS   = 221
    LIST_ITERATE_OR    = 250
    MAPKEY_ITERATE_OR  = 251
    MAPVAL_ITERATE_OR  = 252
    LIST_ITERATE_AND   = 253
    MAPKEY_ITERATE_AND = 254
    MAPVAL_ITERATE_AND = 255

    def self.and(nexp)
      AndOr.new(AND, nexp)
    end

    def self.or(nexp)
      AndOr.new(OR, nexp)
    end

    def self.not
      Op.new(NOT)
    end

    def self.integer_value(value)
      IntegerValue.new(value, INTEGER_VALUE)
    end

    def self.string_value(value)
      StringValue.new(value, STRING_VALUE)
    end

    def self.geojson_value(value)
      GeoJsonValue.new(value, GEOJSON_VALUE)
    end

    def self.integer_bin(name)
      StringValue.new(name, INTEGER_BIN)
    end

    def self.string_bin(name)
      StringValue.new(name, STRING_BIN)
    end

    def self.geojson_bin(name)
      StringValue.new(name, GEOJSON_BIN)
    end

    def self.list_bin(name)
      StringValue.new(name, LIST_BIN)
    end

    def self.map_bin(name)
      StringValue.new(name, MAP_BIN)
    end

    def self.integer_var(name)
      StringValue.new(name, INTEGER_VAR)
    end

    def self.string_var(name)
      StringValue.new(name, STRING_VAR)
    end

    def self.geojson_var(name)
      StringValue.new(name, GEOJSON_VAR)
    end

    def self.last_update
      Op.new(LAST_UPDATE)
    end

    def self.void_time
      Op.new(VOID_TIME)
    end

    def self.integer_equal
      Op.new(INTEGER_EQUAL)
    end

    def self.integer_unequal
      Op.new(INTEGER_UNEQUAL)
    end

    def self.integer_greater
      Op.new(INTEGER_GREATER)
    end

    def self.integer_greater_eq
      Op.new(INTEGER_GREATEREQ)
    end

    def self.integer_less
      Op.new(INTEGER_LESS)
    end

    def self.integer_less_eq
      Op.new(INTEGER_LESSEQ)
    end

    def self.string_equal
      Op.new(STRING_EQUAL)
    end

    def self.string_unequal
      Op.new(STRING_UNEQUAL)
    end

    def self.string_regex(flags)
      Regex.new(STRING_REGEX, flags)
    end

    def self.geojson_within
      Op.new(GEOJSON_WITHIN)
    end

    def self.geojson_contains
      Op.new(GEOJSON_CONTAINS)
    end

    def self.list_iterate_or(var_name)
      StringValue.new(var_name, LIST_ITERATE_OR)
    end

    def self.list_iterate_and(var_name)
      StringValue.new(var_name, LIST_ITERATE_AND)
    end

    def self.mapkey_iterate_or(var_name)
      StringValue.new(var_name, MAPKEY_ITERATE_OR)
    end

    def self.mapkey_iterate_and(var_name)
      StringValue.new(var_name, MAPKEY_ITERATE_AND)
    end

    def self.mapval_iterate_or(var_name)
      StringValue.new(var_name, MAPVAL_ITERATE_OR)
    end

    def self.mapval_iterate_and(var_name)
      StringValue.new(var_name, MAPVAL_ITERATE_AND)
    end



    def self.estimate_size(predexp)
      predexp.map(&:estimate_size).sum
    end

    def self.write(predexp, buffer, offset)
      predexp.each do |p|
        begin
          offset = p.write(buffer, offset)
        rescue => e
          puts "Error: #{e}"
        end

      end

      offset
    end
  end
end
