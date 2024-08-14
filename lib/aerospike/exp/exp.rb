# encoding: utf-8
# Copyright 2014-2023 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may no
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

module Aerospike
  class Exp

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

    # Expression type.
    module Type #:nodoc:
      NIL = 0
      BOOL = 1
      INT = 2
      STRING = 3
      LIST = 4
      MAP = 5
      BLOB = 6
      FLOAT = 7
      GEO = 8
      HLL = 9
    end # module type

    # Expression write flags.
    module WriteFlags
      # Default. Allow create or update.
      DEFAULT = 0

      # If bin does not exist, a new bin will be created.
      # If bin exists, the operation will be denied.
      # If bin exists, fail with ResultCode#BIN_EXISTS_ERROR
      # when #POLICY_NO_FAIL is not set.
      CREATE_ONLY = 1

      # If bin exists, the bin will be overwritten.
      # If bin does not exist, the operation will be denied.
      # If bin does not exist, fail with ResultCode#BIN_NOT_FOUND
      # when #POLICY_NO_FAIL is not set.
      UPDATE_ONLY = 2

      # If expression results in nil value, then delete the bin. Otherwise, fail with
      # ResultCode#OP_NOT_APPLICABLE when #POLICY_NO_FAIL is not set.
      ALLOW_DELETE = 4

      # Do not raise error if operation is denied.
      POLICY_NO_FAIL = 8

      # Ignore failures caused by the expression resolving to unknown or a non-bin type.
      EVAL_NO_FAIL = 16
    end # module WriteFlags

    # Expression write flags.
    module ReadFlags
      # Default.
      DEFAULT = 0

      # Ignore failures caused by the expression resolving to unknown or a non-bin type.
      EVAL_NO_FAIL = 16
    end # module ReadFlags

    # Create record key expression of specified type.
    #
    # ==== Examples
    # # Integer record key >= 100000
    # Exp.ge(Exp.key(Type::INT), Exp.int_val(100000))
    def self.key(type)
      CmdInt.new(KEY, type)
    end

    # Create expression that returns if the primary key is stored in the record meta data
    # as a boolean expression. This would occur when {Policy#send_key}
    # is true on record write. This expression usually evaluates quickly because record
    # meta data is cached in memory.
    #
    # ==== Examples
    #   # Key exists in record meta data
    #   Exp.key_exists
    def self.key_exists
      Cmd.new(KEY_EXISTS)
    end

    #--------------------------------------------------
    # Record Bin
    #--------------------------------------------------

    # Create bin expression of specified type.
    #
    # ==== Examples
    #   # String bin "a" == "views"
    #   Exp.eq(Exp.bin("a", Type::STRING), Exp.str_val("views"))
    def self.bin(name, type)
      Bin.new(name, type)
    end

    # Create 64 bit integer bin expression.
    #
    # ==== Examples
    #   # Integer bin "a" == 200
    #   Exp.eq(Exp.int_bin("a"), Exp.val(200))
    def self.int_bin(name)
      Bin.new(name, Type::INT)
    end

    # Create 64 bit float bin expression.
    #
    # ==== Examples
    #   # Float bin "a" >= 1.5
    #   Exp.ge(Exp.float_bin("a"), Exp.int_val(1.5))
    def self.float_bin(name)
      Bin.new(name, Type::FLOAT)
    end

    # Create string bin expression.
    #
    # ==== Examples
    #   # String bin "a" == "views"
    #   Exp.eq(Exp.str_bin("a"), Exp.str_val("views"))
    def self.str_bin(name)
      Bin.new(name, Type::STRING)
    end

    # Create boolean bin expression.
    #
    # ==== Examples
    #   # Boolean bin "a" == true
    #   Exp.eq(Exp.bool_bin("a"), Exp.val(true))
    def self.bool_bin(name)
      Bin.new(name, Type::BOOL)
    end

    # Create bin expression.
    #
    # ==== Examples
    #   # Blob bin "a" == [1,2,3]
    #   Exp.eq(Exp.blob_bin("a"), Exp.val(new {1, 2, 3}))
    def self.blob_bin(name)
      Bin.new(name, Type::BLOB)
    end

    # Create geospatial bin expression.
    #
    # ==== Examples
    #   # Geo bin "a" == region
    #   String region = "{ \"type\": \"AeroCircle\", \"coordinates\": [[-122.0, 37.5], 50000.0] }"
    #   Exp.geo_compare(Exp.geo_bin("loc"), Exp.geo(region))
    def self.geo_bin(name)
      Bin.new(name, Type::GEO)
    end

    # Create list bin expression.
    #
    # ==== Examples
    #   # Bin a[2] == 3
    #   Exp.eq(ListExp.get_by_index(ListReturnType::VALUE, Type::INT, Exp.val(2), Exp.list_bin("a")), Exp.val(3))
    def self.list_bin(name)
      Bin.new(name, Type::LIST)
    end

    # Create map bin expression.
    #
    # ==== Examples
    #   # Bin a["key"] == "value"
    #   Exp.eq(
    #     MapExp.get_by_key(MapReturnType::VALUE, Type::STRING, Exp.str_val("key"), Exp.map_bin("a")),
    #     Exp.str_val("value"))
    def self.map_bin(name)
      Bin.new(name, Type::MAP)
    end

    # Create hll bin expression.
    #
    # ==== Examples
    # # HLL bin "a" count > 7
    # Exp.gt(HLLExp.get_count(Exp.hll_bin("a")), Exp.val(7))
    def self.hll_bin(name)
      Bin.new(name, Type::HLL)
    end

    # Create expression that returns if bin of specified name exists.
    #
    # ==== Examples
    #   # Bin "a" exists in record
    #   Exp.bin_exists("a")
    def self.bin_exists(name)
      Exp.ne(Exp.bin_type(name), Exp.int_val(0))
    end

    # Create expression that returns bin's integer particle type::
    # See {ParticleType}.
    #
    # ==== Examples
    #   # Bin "a" particle type is a list
    #   Exp.eq(Exp.bin_type("a"), Exp.val(ParticleType::LIST))
    def self.bin_type(name)
      CmdStr.new(BIN_TYPE, name)
    end

    # Create expression that returns the record size. This expression usually evaluates
    # quickly because record meta data is cached in memory.
    # Requires server version 7.0+. This expression replaces {#deviceSize()} and
    # {#memorySize()} since those older expressions are equivalent on server version 7.0+.
    #
    # {@code
    # // Record size >= 100 KB
    # Exp.ge(Exp.record_size(), Exp.val(100 * 1024))
    # }
    def self.record_size
      Cmd.new(RECORD_SIZE)
    end


    #--------------------------------------------------
    # Misc
    #--------------------------------------------------

    # Create expression that returns record set name string. This expression usually
    # evaluates quickly because record meta data is cached in memory.
    #
    # ==== Examples
    #   # Record set name == "myset"
    #   Exp.eq(Exp.set_name, Exp.str_val("myset"))
    def self.set_name
      Cmd.new(SET_NAME)
    end

    # Create expression that returns record size on disk. If server storage-engine is
    # memory, then zero is returned. This expression usually evaluates quickly because
    # record meta data is cached in memory.
    #
    # ==== Examples
    #   # Record device size >= 100 KB
    #   Exp.ge(Exp.device_size, Exp.int_val(100 * 1024))
    def self.device_size
      Cmd.new(DEVICE_SIZE)
    end

    # Create expression that returns record size in memory. If server storage-engine is
    # not memory nor data-in-memory, then zero is returned. This expression usually evaluates
    # quickly because record meta data is cached in memory.
    #
    # Requires server version 5.3.0+
    #
    # ==== Examples
    #   # Record memory size >= 100 KB
    #   Exp.ge(Exp.memory_size, Exp.int_val(100 * 1024))
    def self.memory_size
      Cmd.new(MEMORY_SIZE)
    end

    # Create expression that returns record last update time expressed as 64 bit integer
    # nanoseconds since 1970-01-01 epoch. This expression usually evaluates quickly because
    # record meta data is cached in memory.
    #
    # ==== Examples
    #   # Record last update time >= 2020-01-15
    #   Exp.ge(Exp.last_update, Exp.val(new GregorianCalendar(2020, 0, 15)))
    def self.last_update
      Cmd.new(LAST_UPDATE)
    end

    # Create expression that returns milliseconds since the record was last updated.
    # This expression usually evaluates quickly because record meta data is cached in memory.
    #
    # ==== Examples
    #   # Record last updated more than 2 hours ago
    #   Exp.gt(Exp.since_update, Exp.val(2 * 60 * 60 * 1000))
    def self.since_update
      Cmd.new(SINCE_UPDATE)
    end

    # Create expression that returns record expiration time expressed as 64 bit integer
    # nanoseconds since 1970-01-01 epoch. This expression usually evaluates quickly because
    # record meta data is cached in memory.
    #
    # ==== Examples
    #   # Record expires on 2021-01-01
    #   Exp.and(
    #     Exp.ge(Exp.void_time, Exp.val(new GregorianCalendar(2021, 0, 1))),
    #     Exp.lt(Exp.void_time, Exp.val(new GregorianCalendar(2021, 0, 2))))
    def self.void_time
      Cmd.new(VOID_TIME)
    end

    # Create expression that returns record expiration time (time to live) in integer seconds.
    # This expression usually evaluates quickly because record meta data is cached in memory.
    #
    # ==== Examples
    #   # Record expires in less than 1 hour
    #   Exp.lt(Exp.ttl, Exp.val(60 * 60))
    def self.ttl
      Cmd.new(TTL)
    end

    # Create expression that returns if record has been deleted and is still in tombstone state.
    # This expression usually evaluates quickly because record meta data is cached in memory.
    #
    # ==== Examples
    #   # Deleted records that are in tombstone state.
    #   Exp.is_tombstone
    def self.is_tombstone
      Cmd.new(IS_TOMBSTONE)
    end

    # Create expression that returns record digest modulo as integer. This expression usually
    # evaluates quickly because record meta data is cached in memory.
    #
    # ==== Examples
    #   # Records that have digest(key) % 3 == 1
    #   Exp.eq(Exp.digest_modulo(3), Exp.int_val(1))
    def self.digest_modulo(mod)
      CmdInt.new(DIGEST_MODULO, mod)
    end

    # Create expression that performs a regex match on a string bin or string value expression.
    #
    # ==== Examples
    # # Select string bin "a" that starts with "prefix" and ends with "suffix".
    # # Ignore case and do not match newline.
    # Exp.regex_compare("prefix.*suffix", RegexFlags.ICASE | RegexFlags.NEWLINE, Exp.str_bin("a"))
    #
    # @param regex		regular expression string
    # @param flags		regular expression bit flags. See {Exp::RegexFlags}
    # @param bin		string bin or string value expression
    def self.regex_compare(regex, flags, bin)
      Regex.new(bin, regex, flags)
    end

    #--------------------------------------------------
    # GEO Spatial
    #--------------------------------------------------

    # Create compare geospatial operation.
    #
    # ==== Examples
    # # Query region within coordinates.
    #   region =
    #      "{ " +
    #      "  \"type\": \"Polygon\", " +
    #      "  \"coordinates\": [ " +
    #      "    [[-122.500000, 37.000000],[-121.000000, 37.000000], " +
    #      "     [-121.000000, 38.080000],[-122.500000, 38.080000], " +
    #      "     [-122.500000, 37.000000]] " +
    #      "    ] " +
    #      "}"
    #      Exp.geo_compare(Exp.geo_bin("a"), Exp.geo(region))
    def self.geo_compare(left, right)
      CmdExp.new(GEO, left, right)
    end

    # Create geospatial json string value.
    def self.geo(val)
      Geo.new(val)
    end

    #--------------------------------------------------
    # Value
    #--------------------------------------------------

    # Create boolean value.
    def self.bool_val(val)
      Bool.new(val)
    end

    # Create 64 bit integer value.
    def self.int_val(val)
      Int.new(val)
    end

    # Create 64 bit floating point value.
    def self.float_val(val)
      Float.new(val)
    end

    # Create string value.
    def self.str_val(val)
      Str.new(val)
    end

    # Create blob byte value.
    def self.blob_val(val)
      Blob.new(val)
    end

    # Create list value.
    def self.list_val(*list)
      ListVal.new(list)
    end

    # Create map value.
    def self.map_val(map)
      MapVal.new(map)
    end

    # Create nil value.
    def self.nil_val
      Nil.new
    end

    # Create Infinity value.
    def self.infinity_val
      Infinity.new
    end

    # Create Wildcard value.
    def self.wildcard_val
      Wildcard.new
    end

    #--------------------------------------------------
    # Boolean Operator
    #--------------------------------------------------

    # Create "not" operator expression.
    #
    # ==== Examples
    #   # ! (a == 0 || a == 10)
    #   Exp.not(
    #     Exp.or(
    #       Exp.eq(Exp.int_bin("a"), Exp.val(0)),
    #       Exp.eq(Exp.int_bin("a"), Exp.int_val(10))))
    def self.not(exp)
      CmdExp.new(NOT, exp)
    end

    # Create "and" (&&) operator that applies to a variable number of expressions.
    #
    # ==== Examples
    #   # (a > 5 || a == 0) && b < 3
    #   Exp.and(
    #     Exp.or(
    #       Exp.gt(Exp.int_bin("a"), Exp.val(5)),
    #       Exp.eq(Exp.int_bin("a"), Exp.val(0))),
    #     Exp.lt(Exp.int_bin("b"), Exp.val(3)))
    def self.and(*exps)
      CmdExp.new(AND, *exps)
    end

    # Create "or" (||) operator that applies to a variable number of expressions.
    #
    # ==== Examples
    #   # a == 0 || b == 0
    #   Exp.or(
    #     Exp.eq(Exp.int_bin("a"), Exp.val(0)),
    #     Exp.eq(Exp.int_bin("b"), Exp.val(0)))
    def self.or(*exps)
      CmdExp.new(OR, *exps)
    end

    # Create expression that returns true if only one of the expressions are true.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # exclusive(a == 0, b == 0)
    #   Exp.exclusive(
    #     Exp.eq(Exp.int_bin("a"), Exp.val(0)),
    #     Exp.eq(Exp.int_bin("b"), Exp.val(0)))
    def self.exclusive(*exps)
      CmdExp.new(EXCLUSIVE, *exps)
    end

    # Create equal (==) expression.
    #
    # ==== Examples
    #   # a == 11
    #   Exp.eq(Exp.int_bin("a"), Exp.int_val(11))
    def self.eq(left, right)
      CmdExp.new(EQ, left, right)
    end

    # Create not equal (!=) expression
    #
    # ==== Examples
    #   # a != 13
    #   Exp.ne(Exp.int_bin("a"), Exp.int_val(13))
    def self.ne(left, right)
      CmdExp.new(NE, left, right)
    end

    # Create greater than (>) operation.
    #
    # ==== Examples
    #   # a > 8
    #   Exp.gt(Exp.int_bin("a"), Exp.val(8))
    def self.gt(left, right)
      CmdExp.new(GT, left, right)
    end

    # Create greater than or equal (>=) operation.
    #
    # ==== Examples
    #   # a >= 88
    #   Exp.ge(Exp.int_bin("a"), Exp.val(88))
    def self.ge(left, right)
      CmdExp.new(GE, left, right)
    end

    # Create less than (<) operation.
    #
    # ==== Examples
    #   # a < 1000
    #   Exp.lt(Exp.int_bin("a"), Exp.int_val(1000))
    def self.lt(left, right)
      CmdExp.new(LT, left, right)
    end

    # Create less than or equals (<=) operation.
    #
    # ==== Examples
    #   # a <= 1
    #   Exp.le(Exp.int_bin("a"), Exp.int_val(1))
    def self.le(left, right)
      CmdExp.new(LE, left, right)
    end

    #--------------------------------------------------
    # Number Operator
    #--------------------------------------------------

    # Create "add" (+) operator that applies to a variable number of expressions.
    # Return sum of all arguments. All arguments must resolve to the same type (or float).
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # a + b + c == 10
    #   Exp.eq(
    #     Exp.add(Exp.int_bin("a"), Exp.int_bin("b"), Exp.int_bin("c")),
    #     Exp.int_val(10))
    def self.add(*exps)
      CmdExp.new(ADD, *exps)
    end

    # Create "subtract" (-) operator that applies to a variable number of expressions.
    # If only one argument is provided, return the negation of that argument.
    # Otherwise, return the sum of the 2nd to Nth argument subtracted from the 1st
    # argument. All arguments must resolve to the same type (or float).
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # a - b - c > 10
    #   Exp.gt(
    #     Exp.sub(Exp.int_bin("a"), Exp.int_bin("b"), Exp.int_bin("c")),
    #     Exp.int_val(10))
    def self.sub(*exps)
      CmdExp.new(SUB, *exps)
    end

    # Create "multiply" (*) operator that applies to a variable number of expressions.
    # Return the product of all arguments. If only one argument is supplied, return
    # that argument. All arguments must resolve to the same type (or float).
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # a * b * c < 100
    #   Exp.lt(
    #     Exp.mul(Exp.int_bin("a"), Exp.int_bin("b"), Exp.int_bin("c")),
    #     Exp.int_val(100))
    def self.mul(*exps)
      CmdExp.new(MUL, *exps)
    end

    # Create "divide" (/) operator that applies to a variable number of expressions.
    # If there is only one argument, returns the reciprocal for that argument.
    # Otherwise, return the first argument divided by the product of the rest.
    # All arguments must resolve to the same type (or float).
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # a / b / c > 1
    #   Exp.gt(
    #     Exp.div(Exp.int_bin("a"), Exp.int_bin("b"), Exp.int_bin("c")),
    #     Exp.int_val(1))
    def self.div(*exps)
      CmdExp.new(DIV, *exps)
    end

    # Create "power" operator that raises a "base" to the "exponent" power.
    # All arguments must resolve to floats.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # pow(a, 2.0) == 4.0
    #   Exp.eq(
    #     Exp.pow(Exp.float_bin("a"), Exp.val(2.0)),
    #     Exp.val(4.0))
    def self.pow(base, exponent)
      CmdExp.new(POW, base, exponent)
    end

    # Create "log" operator for logarithm of "num" with base "base".
    # All arguments must resolve to floats.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # log(a, 2.0) == 4.0
    #   Exp.eq(
    #     Exp.log(Exp.float_bin("a"), Exp.val(2.0)),
    #     Exp.val(4.0))
    def self.log(num, base)
      CmdExp.new(LOG, num, base)
    end

    # Create "modulo" (%) operator that determines the remainder of "numerator"
    # divided by "denominator". All arguments must resolve to integers.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # a % 10 == 0
    #   Exp.eq(
    #     Exp.mod(Exp.int_bin("a"), Exp.int_val(10)),
    #     Exp.val(0))
    def self.mod(numerator, denominator)
      CmdExp.new(MOD, numerator, denominator)
    end

    # Create operator that returns absolute value of a number.
    # All arguments must resolve to integer or float.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # abs(a) == 1
    #   Exp.eq(
    #     Exp.abs(Exp.int_bin("a")),
    #     Exp.int_val(1))
    def self.abs(value)
      CmdExp.new(ABS, value)
    end

    # Create expression that rounds a floating point number down to the closest integer value.
    # The return type is float. Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # floor(2.95) == 2.0
    #   Exp.eq(
    #     Exp.floor(Exp.val(2.95)),
    #     Exp.val(2.0))
    def self.floor(num)
      CmdExp.new(FLOOR, num)
    end

    # Create expression that rounds a floating point number up to the closest integer value.
    # The return type is float. Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # ceil(2.15) >= 3.0
    #   Exp.ge(
    #     Exp.ceil(Exp.val(2.15)),
    #     Exp.val(3.0))
    def self.ceil(num)
      CmdExp.new(CEIL, num)
    end

    # Create expression that converts a float to an integer.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # int(2.5) == 2
    #   Exp.eq(
    #     Exp.to_int(Exp.val(2.5)),
    #     Exp.val(2))
    def self.to_int(num)
      CmdExp.new(TO_INT, num)
    end

    # Create expression that converts an integer to a float.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # float(2) == 2.0
    #   Exp.eq(
    #     Exp.to_float(Exp.val(2))),
    #     Exp.val(2.0))
    def self.to_float(num)
      CmdExp.new(TO_FLOAT, num)
    end

    # Create integer "and" (&) operator that is applied to two or more integers.
    # All arguments must resolve to integers.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # a & 0xff == 0x11
    #   Exp.eq(
    #     Exp.int_and(Exp.int_bin("a"), Exp.val(0xff)),
    #     Exp.val(0x11))
    def self.int_and(*exps)
      CmdExp.new(INT_AND, *exps)
    end

    # Create integer "or" (|) operator that is applied to two or more integers.
    # All arguments must resolve to integers.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # a | 0x10 != 0
    #   Exp.ne(
    #     Exp.int_or(Exp.int_bin("a"), Exp.val(0x10)),
    #     Exp.val(0))
    def self.int_or(*exps)
      CmdExp.new(INT_OR, *exps)
    end

    # Create integer "xor" (^) operator that is applied to two or more integers.
    # All arguments must resolve to integers.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # a ^ b == 16
    #   Exp.eq(
    #     Exp.int_xor(Exp.int_bin("a"), Exp.int_bin("b")),
    #     Exp.int_val(16))
    def self.int_xor(*exps)
      CmdExp.new(INT_XOR, *exps)
    end

    # Create integer "not" (~) operator.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # ~a == 7
    #   Exp.eq(
    #     Exp.int_not(Exp.int_bin("a")),
    #     Exp.val(7))
    def self.int_not(exp)
      CmdExp.new(INT_NOT, exp)
    end

    # Create integer "left shift" (<<) operator.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # a << 8 > 0xff
    #   Exp.gt(
    #     Exp.lshift(Exp.int_bin("a"), Exp.val(8)),
    #     Exp.val(0xff))
    def self.lshift(value, shift)
      CmdExp.new(INT_LSHIFT, value, shift)
    end

    # Create integer "logical right shift" (>>>) operator.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # a >>> 8 > 0xff
    #   Exp.gt(
    #     Exp.rshift(Exp.int_bin("a"), Exp.val(8)),
    #     Exp.val(0xff))
    def self.rshift(value, shift)
      CmdExp.new(INT_RSHIFT, value, shift)
    end

    # Create integer "arithmetic right shift" (>>) operator.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # a >> 8 > 0xff
    #   Exp.gt(
    #     Exp.arshift(Exp.int_bin("a"), Exp.val(8)),
    #     Exp.val(0xff))
    def self.arshift(value, shift)
      CmdExp.new(INT_ARSHIFT, value, shift)
    end

    # Create expression that returns count of integer bits that are set to 1.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # count(a) == 4
    #   Exp.eq(
    #     Exp.count(Exp.int_bin("a")),
    #     Exp.val(4))
    def self.count(exp)
      CmdExp.new(INT_COUNT, exp)
    end

    # Create expression that scans integer bits from left (most significant bit) to
    # right (least significant bit), looking for a search bit value. When the
    # search value is found, the index of that bit (where the most significant bit is
    # index 0) is returned. If "search" is true, the scan will search for the bit
    # value 1. If "search" is false it will search for bit value 0.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # lscan(a, true) == 4
    #   Exp.eq(
    #     Exp.lscan(Exp.int_bin("a"), Exp.val(true)),
    #     Exp.val(4))
    def self.lscan(value, search)
      CmdExp.new(INT_LSCAN, value, search)
    end

    # Create expression that scans integer bits from right (least significant bit) to
    # left (most significant bit), looking for a search bit value. When the
    # search value is found, the index of that bit (where the most significant bit is
    # index 0) is returned. If "search" is true, the scan will search for the bit
    # value 1. If "search" is false it will search for bit value 0.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # rscan(a, true) == 4
    #   Exp.eq(
    #     Exp.rscan(Exp.int_bin("a"), Exp.val(true)),
    #     Exp.val(4))
    def self.rscan(value, search)
      CmdExp.new(INT_RSCAN, value, search)
    end

    # Create expression that returns the minimum value in a variable number of expressions.
    # All arguments must be the same type (or float).
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # min(a, b, c) > 0
    #   Exp.gt(
    #     Exp.min(Exp.int_bin("a"), Exp.int_bin("b"), Exp.int_bin("c")),
    #     Exp.val(0))
    def self.min(*exps)
      CmdExp.new(MIN, *exps)
    end

    # Create expression that returns the maximum value in a variable number of expressions.
    # All arguments must be the same type (or float).
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    #   # max(a, b, c) > 100
    #   Exp.gt(
    #     Exp.max(Exp.int_bin("a"), Exp.int_bin("b"), Exp.int_bin("c")),
    #     Exp.int_val(100))
    def self.max(*exps)
      CmdExp.new(MAX, *exps)
    end

    #--------------------------------------------------
    # Variables
    #--------------------------------------------------

    # Conditionally select an expression from a variable number of expression pairs
    # followed by default expression action. Requires server version 5.6.0+.
    #
    # ==== Examples
    # Args Format: bool exp1, action exp1, bool exp2, action exp2, ..., action-default
    #
    # # Apply operator based on type::
    #   Exp.cond(
    #     Exp.eq(Exp.int_bin("type"), Exp.val(0)), Exp.add(Exp.int_bin("val1"), Exp.int_bin("val2")),
    #     Exp.eq(Exp.int_bin("type"), Exp.int_val(1)), Exp.sub(Exp.int_bin("val1"), Exp.int_bin("val2")),
    #     Exp.eq(Exp.int_bin("type"), Exp.val(2)), Exp.mul(Exp.int_bin("val1"), Exp.int_bin("val2")),
    #     Exp.val(-1))
    def self.cond(*exps)
      CmdExp.new(COND, *exps)
    end

    # Define variables and expressions in scope.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    # Args Format: <def1>, <def2>, ..., <exp>
    # def: {Exp#def(String, Exp)}
    # exp: Scoped expression
    #
    # ==== Examples
    # # 5 < a < 10
    # Exp.let(
    #   Exp.def("x", Exp.int_bin("a")),
    #   Exp.and(
    #     Exp.lt(Exp.val(5), Exp.var("x")),
    #     Exp.lt(Exp.var("x"), Exp.int_val(10))))
    def self.let(*exps)
      Let.new(exps)
    end

    # Assign variable to a {Exp#let(Exp...)} expression that can be accessed later.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    # # 5 < a < 10
    # Exp.let(
    #   Exp.def("x", Exp.int_bin("a")),
    #   Exp.and(
    #     Exp.lt(Exp.val(5), Exp.var("x")),
    #     Exp.lt(Exp.var("x"), Exp.int_val(10))))
    def self.def(name, value)
      Def.new(name, value)
    end

    # Retrieve expression value from a variable.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    # # 5 < a < 10
    # Exp.let(
    #   Exp.def("x", Exp.int_bin("a")),
    #   Exp.and(
    #     Exp.lt(Exp.val(5), Exp.var("x")),
    #     Exp.lt(Exp.var("x"), Exp.int_val(10))))
    def self.var(name)
      CmdStr.new(VAR, name)
    end

    #--------------------------------------------------
    # Miscellaneous
    #--------------------------------------------------

    # Create unknown value. Used to intentionally fail an expression.
    # The failure can be ignored with {Exp::WriteFlags#EVAL_NO_FAIL}
    # or {Exp::ReadFlags#EVAL_NO_FAIL}.
    # Requires server version 5.6.0+.
    #
    # ==== Examples
    # # double v = balance - 100.0
    # # return (v > 0.0)? v : unknown
    # Exp.let(
    #   Exp.def("v", Exp.sub(Exp.float_bin("balance"), Exp.int_val(100.0))),
    #   Exp.cond(
    #     Exp.ge(Exp.var("v"), Exp.val(0.0)), Exp.var("v"),
    #     Exp.unknown))
    def self.unknown
      Cmd.new(UNKNOWN)
    end

    # # Merge precompiled expression into a new expression tree.
    # # Useful for storing common precompiled expressions and then reusing
    # # these expressions as part of a greater expression.
    # #
    # # ==== Examples
    # # # Merge precompiled expression into new expression.
    # # Expression e = Exp.build(Exp.eq(Exp.int_bin("a"), Exp.val(200)))
    # # Expression merged = Exp.build(Exp.and(Exp.expr(e), Exp.eq(Exp.int_bin("b"), Exp.int_val(100))))
    # def self.expr(Expression e)
    #   new ExpBytes.new(e)
    # end

    #--------------------------------------------------
    # Internal
    #--------------------------------------------------
    MODIFY = 0x40

    def bytes
      if @bytes.nil?
        Packer.use do |packer|
          pack(packer)
          @bytes = packer.bytes
        end
      end
      @bytes
    end

    # Estimate expression size in wire protocol.
    # For internal use only.
    def size
      bytes.length
    end

    # Write expression in wire protocol.
    # For internal use only.
    def write(buf, offset)
      buf.write_binary(bytes, offset)
    end

    private

    UNKNOWN = 0
    EQ = 1
    NE = 2
    GT = 3
    GE = 4
    LT = 5
    LE = 6
    REGEX = 7
    GEO = 8
    AND = 16
    OR = 17
    NOT = 18
    EXCLUSIVE = 19
    ADD = 20
    SUB = 21
    MUL = 22
    DIV = 23
    POW = 24
    LOG = 25
    MOD = 26
    ABS = 27
    FLOOR = 28
    CEIL = 29
    TO_INT = 30
    TO_FLOAT = 31
    INT_AND = 32
    INT_OR = 33
    INT_XOR = 34
    INT_NOT = 35
    INT_LSHIFT = 36
    INT_RSHIFT = 37
    INT_ARSHIFT = 38
    INT_COUNT = 39
    INT_LSCAN = 40
    INT_RSCAN = 41
    MIN = 50
    MAX = 51
    DIGEST_MODULO = 64
    DEVICE_SIZE = 65
    LAST_UPDATE = 66
    SINCE_UPDATE = 67
    VOID_TIME = 68
    TTL = 69
    SET_NAME = 70
    KEY_EXISTS = 71
    IS_TOMBSTONE = 72
    MEMORY_SIZE = 73
    RECORD_SIZE = 74
    KEY = 80
    BIN = 81
    BIN_TYPE = 82
    COND = 123
    VAR = 124
    LET = 125
    QUOTED = 126
    CALL = 127
    NANOS_PER_MILLIS = 1000000

    def self.pack(ctx, command, *vals)
      Packer.use do |packer|
        # ctx is not support for bit commands
        packer.write_array_header(vals.to_a.length + 1)
        packer.write(command)
        vals.each do |v|
          if v.is_a?(Exp)
            v.pack(packer)
          else
            Value.of(v).pack(packer)
          end
        end
        return packer.bytes
      end
    end

    def self.pack_ctx(packer, ctx)
      unless ctx.to_a.empty?
        packer.write_array_header(3)
        packer.write(0xff)
        packer.write_array_header(ctx.length * 2)

        ctx.each do |c|
          packer.write(c.id)
          c.value.pack(packer)
        end
      end
    end

    # For internal use only.
    class Module < Exp
      attr_reader :bin, :bytes, :ret_type, :module

      def initialize(bin, bytes, ret_type, modul)
        @bin = bin
        @bytes = bytes
        @ret_type = ret_type
        @module = modul
      end

      def pack(packer)
        packer.write_array_header(5)
        packer.write(Exp::CALL)
        packer.write(@ret_type)
        packer.write(@module)
        # packer.pack_byte_array(@bytes, 0, @bytes.length)
        packer.write_raw(@bytes)
        @bin.pack(packer)
      end
    end

    class Bin < Exp
      attr_reader :name, :type

      def initialize(name, type)
        @name = name
        @type = type
      end

      def pack(packer)
        packer.write_array_header(3)
        packer.write(BIN)
        packer.write(@type)
        packer.write(@name)
      end
    end

    class Regex < Exp
      attr_reader :bin, :regex, :flags

      def initialize(bin, regex, flags)
        @bin = bin
        @regex = regex
        @flags = flags
      end

      def pack(packer)
        packer.write_array_header(4)
        packer.write(REGEX)
        packer.write(@flags)
        packer.write(@regex)
        @bin.pack(packer)
      end
    end

    class Let < Exp
      attr_reader :exps

      def initialize(exps)
        @exps = exps
      end

      def pack(packer)
        # Let wire format: LET <defname1>, <defexp1>, <defname2>, <defexp2>, ..., <scope exp>
        count = ((@exps.length - 1) * 2) + 2
        packer.write_array_header(count)
        packer.write(LET)

        @exps.each do |exp|
          exp.pack(packer)
        end
      end
    end

    class Def < Exp
      attr_reader :name, :exp

      def initialize(name, exp)
        @name = name
        @exp = exp
      end

      def pack(packer)
        packer.write(@name)
        @exp.pack(packer)
      end
    end

    class CmdExp < Exp
      attr_reader :exps, :cmd

      def initialize(cmd, *exps)
        @exps = exps
        @cmd = cmd
      end

      def pack(packer)
        packer.write_array_header(@exps.length + 1)
        packer.write(@cmd)
        @exps.each do |exp|
          exp.pack(packer)
        end
      end
    end

    class CmdInt < Exp
      attr_reader :cmd, :val

      def initialize(cmd, val)
        @cmd = cmd
        @val = val
      end

      def pack(packer)
        packer.write_array_header(2)
        Value.of(@cmd).pack(packer)
        Value.of(@val).pack(packer)
      end
    end

    class CmdStr < Exp
      attr_reader :str, :cmd

      def initialize(cmd, str)
        @str = str
        @cmd = cmd
      end

      def pack(packer)
        packer.write_array_header(2)
        Value.of(@cmd).pack(packer)
        packer.write(@str)
      end
    end

    class Cmd < Exp
      attr_reader :cmd

      def initialize(cmd)
        @cmd = cmd
      end

      def pack(packer)
        packer.write_array_header(1)
        packer.write(@cmd)
      end
    end

    class Bool < Exp
      attr_reader :val

      def initialize(val)
        @val = val
      end

      def pack(packer)
        BoolValue.new(@val).pack(packer)
      end
    end

    class Int < Exp
      attr_reader :val

      def initialize(val)
        @val = val.to_i
      end

      def pack(packer)
        IntegerValue.new(@val).pack(packer)
      end
    end

    class Float < Exp
      attr_reader :val

      def initialize(val)
        @val = val.to_f
      end

      def pack(packer)
        FloatValue.new(@val).pack(packer)
      end
    end

    class Str < Exp
      attr_reader :val

      def initialize(val)
        @val = val
      end

      def pack(packer)
        StringValue.new(@val).pack(packer)
      end
    end

    class Geo < Exp
      attr_reader :val

      def initialize(val)
        @val = val
      end

      def pack(packer)
        Value.of(@val).pack(packer)
      end
    end

    class Blob < Exp
      attr_reader :val

      def initialize(val)
        @val = val
      end

      def pack(packer)
        BytesValue.new(@val).pack(packer)
      end
    end

    class ListVal < Exp
      attr_reader :list

      def initialize(list)
        @list = list
      end

      def pack(packer)
        # List values need an extra array and QUOTED in order to distinguish
        # between a multiple argument array call and a local list.
        packer.write_array_header(2)
        packer.write(QUOTED)
        Value.of(@list).pack(packer)
      end
    end

    class MapVal < Exp
      attr_reader :map

      def initialize(map)
        @map = map
      end

      def pack(packer)
        Value.of(@map).pack(packer)
      end
    end

    class Nil < Exp
      def pack(packer)
        Value.of(nil).pack(packer)
      end
    end

    class Infinity < Exp
      def pack(packer)
        InfinityValue.new.pack(packer)
      end
    end

    class Wildcard < Exp
      def pack(packer)
        WildcardValue.new.pack(packer)
      end
    end

    class ExpBytes < Exp
      attr_reader :bytes

      def initialize(e)
        @bytes = e.bytes
      end

      def pack(packer)
        Value.of(@bytes).pack(packer)
      end
    end
  end # class Exp
end # module
