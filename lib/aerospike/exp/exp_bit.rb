# encoding: utf-8
# Copyright 2014-2022 Aerospike, Inc.
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
  # Bit expression generator. See {@link Exp}.
  #
  # The bin expression argument in these methods can be a reference to a bin or the
  # result of another expression. Expressions that modify bin values are only used
  # for temporary expression evaluation and are not permanently applied to the bin.
  # Bit modify expressions the blob bin's value.
  #
  # Offset orientation is left-to-right.  Negative offsets are supported.
  # If the offset is negative, the offset starts backwards from end of the bitmap.
  # If an offset is out of bounds, a parameter error will be returned.
  class Exp::Bit
    # Create expression that resizes _byte[]_ to _byte_size_ according to _resize_flags_ (See {CDT::BitResizeFlags})
    # and returns byte[].
    #
    #   bin = [0b00000001, 0b01000010]
    #   byte_size = 4
    #   resize_flags = 0
    #   returns [0b00000001, 0b01000010, 0b00000000, 0b00000000]
    #
    # ==== Examples
    #   # Resize bin "a" and compare bit count
    #   Exp.eq(
    #     BitExp.count(Exp.val(0), Exp.val(3),
    #       BitExp.resize(BitPolicy.Default, Exp.val(4), 0, Exp.blobBin("a"))),
    #     Exp.val(2))
    def self.resize(byte_size, resize_flags, bin, policy: CDT::BitPolicy::DEFAULT)
      bytes = Exp.pack(nil, RESIZE, byte_size, policy.flags, resize_flags)
      self.add_write(bin, bytes)
    end

    # Create expression that inserts value bytes into byte[] bin at byte_offset and returns byte[].
    #
    # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    # byte_offset = 1
    # value = [0b11111111, 0b11000111]
    # bin result = [0b00000001, 0b11111111, 0b11000111, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    #
    # ==== Examples
    #   # Insert bytes into bin "a" and compare bit count
    #   Exp.eq(
    #     BitExp.count(Exp.val(0), Exp.val(3),
    #       BitExp.insert(BitPolicy.Default, Exp.val(1), Exp.val(bytes), Exp.blobBin("a"))),
    #     Exp.val(2))
    def self.insert(byte_offset, value, bin, policy: CDT::BitPolicy::DEFAULT)
      bytes = Exp.pack(nil, INSERT, byte_offset, value, policy.flags)
      self.add_write(bin, bytes)
    end

    # Create expression that removes bytes from byte[] bin at byte_offset for byte_size and returns byte[].
    #
    # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    # byte_offset = 2
    # byte_size = 3
    # bin result = [0b00000001, 0b01000010]
    #
    # ==== Examples
    #   # Remove bytes from bin "a" and compare bit count
    #   Exp.eq(
    #     BitExp.count(Exp.val(0), Exp.val(3),
    #       BitExp.remove(BitPolicy.Default, Exp.val(2), Exp.val(3), Exp.blobBin("a"))),
    #     Exp.val(2))
    def self.remove(byte_offset, byte_size, bin, policy: CDT::BitPolicy::DEFAULT)
      bytes = Exp.pack(nil, REMOVE, byte_offset, byte_size, policy.flags)
      self.add_write(bin, bytes)
    end

    # Create expression that sets value on byte[] bin at bit_offset for bit_size and returns byte[].
    #
    # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    # bit_offset = 13
    # bit_size = 3
    # value = [0b11100000]
    # bin result = [0b00000001, 0b01000111, 0b00000011, 0b00000100, 0b00000101]
    #
    # ==== Examples
    #   # Set bytes in bin "a" and compare bit count
    #   Exp.eq(
    #     BitExp.count(Exp.val(0), Exp.val(3),
    #       BitExp.set(BitPolicy.Default, Exp.val(13), Exp.val(3), Exp.val(bytes), Exp.blobBin("a"))),
    #     Exp.val(2))
    def self.set(bit_offset, bit_size, value, bin, policy: CDT::BitPolicy::DEFAULT)
      bytes = Exp.pack(nil, SET, bit_offset, bit_size, value, policy.flags)
      self.add_write(bin, bytes)
    end

    # Create expression that performs bitwise "or" on value and byte[] bin at bit_offset for bit_size
    # and returns byte[].
    #
    # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    # bit_offset = 17
    # bit_size = 6
    # value = [0b10101000]
    # bin result = [0b00000001, 0b01000010, 0b01010111, 0b00000100, 0b00000101]
    #
    def self.or(bit_offset, bit_size, value, bin, policy: CDT::BitPolicy::DEFAULT)
      bytes = Exp.pack(nil, OR, bit_offset, bit_size, value, policy.flags)
      self.add_write(bin, bytes)
    end

    # Create expression that performs bitwise "xor" on value and byte[] bin at bit_offset for bit_size
    # and returns byte[].
    #
    # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    # bit_offset = 17
    # bit_size = 6
    # value = [0b10101100]
    # bin result = [0b00000001, 0b01000010, 0b01010101, 0b00000100, 0b00000101]
    #
    def self.xor(bit_offset, bit_size, value, bin, policy: CDT::BitPolicy::DEFAULT)
      bytes = Exp.pack(nil, XOR, bit_offset, bit_size, value, policy.flags)
      self.add_write(bin, bytes)
    end

    # Create expression that performs bitwise "and" on value and byte[] bin at bit_offset for bit_size
    # and returns byte[].
    #
    # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    # bit_offset = 23
    # bit_size = 9
    # value = [0b00111100, 0b10000000]
    # bin result = [0b00000001, 0b01000010, 0b00000010, 0b00000000, 0b00000101]
    #
    def self.and(bit_offset, bit_size, value, bin, policy: CDT::BitPolicy::DEFAULT)
      bytes = Exp.pack(nil, AND, bit_offset, bit_size, value, policy.flags)
      self.add_write(bin, bytes)
    end

    # Create expression that negates byte[] bin starting at bit_offset for bit_size and returns byte[].
    #
    # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    # bit_offset = 25
    # bit_size = 6
    # bin result = [0b00000001, 0b01000010, 0b00000011, 0b01111010, 0b00000101]
    #
    def self.not(bit_offset, bit_size, bin, policy: CDT::BitPolicy::DEFAULT)
      bytes = Exp.pack(nil, NOT, bit_offset, bit_size, policy.flags)
      self.add_write(bin, bytes)
    end

    # Create expression that shifts left byte[] bin starting at bit_offset for bit_size and returns byte[].
    #
    # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    # bit_offset = 32
    # bit_size = 8
    # shift = 3
    # bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00101000]
    #
    def self.lshift(bit_offset, bit_size, shift, bin, policy: CDT::BitPolicy::DEFAULT)
      bytes = Exp.pack(nil, LSHIFT, bit_offset, bit_size, shift, policy.flags)
      self.add_write(bin, bytes)
    end

    # Create expression that shifts right byte[] bin starting at bit_offset for bit_size and returns byte[].
    #
    # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    # bit_offset = 0
    # bit_size = 9
    # shift = 1
    # bin result = [0b00000000, 0b11000010, 0b00000011, 0b00000100, 0b00000101]
    #
    def self.rshift(bit_offset, bit_size, shift, bin, policy: CDT::BitPolicy::DEFAULT)
      bytes = Exp.pack(nil, RSHIFT, bit_offset, bit_size, shift, policy.flags)
      self.add_write(bin, bytes)
    end

    # Create expression that adds value to byte[] bin starting at bit_offset for bit_size and returns byte[].
    # BitSize must be <= 64. Signed indicates if bits should be treated as a signed number.
    # If add overflows/underflows, {@link BitOverflowAction} is used.
    #
    # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    # bit_offset = 24
    # bit_size = 16
    # value = 128
    # signed = false
    # bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b10000101]
    #
    def self.add(bit_offset, bit_size, value, signed, bit_overflow_action, bin, policy: CDT::BitPolicy::DEFAULT)
      bytes = self.pack_math(ADD, policy, bit_offset, bit_size, value, signed, bit_overflow_action)
      self.add_write(bin, bytes)
    end

    # Create expression that subtracts value from byte[] bin starting at bit_offset for bit_size and returns byte[].
    # BitSize must be <= 64. Signed indicates if bits should be treated as a signed number.
    # If add overflows/underflows, {@link BitOverflowAction} is used.
    #
    # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    # bit_offset = 24
    # bit_size = 16
    # value = 128
    # signed = false
    # bin result = [0b00000001, 0b01000010, 0b00000011, 0b0000011, 0b10000101]
    #
    def self.subtract(bit_offset, bit_size, value, signed, bit_overflow_action, bin, policy: CDT::BitPolicy::DEFAULT)
      bytes = self.pack_math(SUBTRACT, policy, bit_offset, bit_size, value, signed, bit_overflow_action)
      self.add_write(bin, bytes)
    end

    # Create expression that sets value to byte[] bin starting at bit_offset for bit_size and returns byte[].
    # BitSize must be <= 64.
    #
    # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    # bit_offset = 1
    # bit_size = 8
    # value = 127
    # bin result = [0b00111111, 0b11000010, 0b00000011, 0b0000100, 0b00000101]
    #
    def self.set_int(bit_offset, bit_size, value, bin, policy: CDT::BitPolicy::DEFAULT)
      bytes = Exp.pack(nil, SET_INT, bit_offset, bit_size, value, policy.flags)
      self.add_write(bin, bytes)
    end

    # Create expression that returns bits from byte[] bin starting at bit_offset for bit_size.
    #
    # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    # bit_offset = 9
    # bit_size = 5
    # returns [0b10000000]
    #
    # ==== Examples
    #   # Bin "a" bits = [0b10000000]
    #   Exp.eq(
    #     BitExp.get(Exp.val(9), Exp.val(5), Exp.blobBin("a")),
    #     Exp.val(new byte[] {(byte)0b10000000}))
    def self.get(bit_offset, bit_size, bin)
      bytes = Exp.pack(nil, GET, bit_offset, bit_size)
      self.add_read(bin, bytes, Exp::Type::BLOB)
    end

    # Create expression that returns integer count of set bits from byte[] bin starting at
    # bit_offset for bit_size.
    #
    # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    # bit_offset = 20
    # bit_size = 4
    # returns 2
    #
    # ==== Examples
    #   # Bin "a" bit count <= 2
    #   Exp.le(BitExp.count(Exp.val(0), Exp.val(5), Exp.blobBin("a")), Exp.val(2))
    def self.count(bit_offset, bit_size, bin)
      bytes = Exp.pack(nil, COUNT, bit_offset, bit_size)
      self.add_read(bin, bytes, Exp::Type::INT)
    end

    # Create expression that returns integer bit offset of the first specified value bit in byte[] bin
    # starting at bit_offset for bit_size.
    #
    # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    # bit_offset = 24
    # bit_size = 8
    # value = true
    # returns 5
    #
    # ==== Examples
    #   # lscan(a) == 5
    #   Exp.eq(BitExp.lscan(Exp.val(24), Exp.val(8), Exp.val(true), Exp.blobBin("a")), Exp.val(5))
    #
    # @param bit_offset		offset int expression
    # @param bit_size		size int expression
    # @param value			boolean expression
    # @param bin			bin or blob value expression
    def self.lscan(bit_offset, bit_size, value, bin)
      bytes = Exp.pack(nil, LSCAN, bit_offset, bit_size, value)
      self.add_read(bin, bytes, Exp::Type::INT)
    end

    # Create expression that returns integer bit offset of the last specified value bit in byte[] bin
    # starting at bit_offset for bit_size.
    # Example:
    #
    # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    # bit_offset = 32
    # bit_size = 8
    # value = true
    # returns 7
    #
    # ==== Examples
    #   # rscan(a) == 7
    #   Exp.eq(BitExp.rscan(Exp.val(32), Exp.val(8), Exp.val(true), Exp.blobBin("a")), Exp.val(7))
    #
    # @param bit_offset		offset int expression
    # @param bit_size		size int expression
    # @param value			boolean expression
    # @param bin			bin or blob value expression
    def self.rscan(bit_offset, bit_size, value, bin)
      bytes = Exp.pack(nil, RSCAN, bit_offset, bit_size, value)
      self.add_read(bin, bytes, Exp::Type::INT)
    end

    # Create expression that returns integer from byte[] bin starting at bit_offset for bit_size.
    # Signed indicates if bits should be treated as a signed number.
    #
    # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    # bit_offset = 8
    # bit_size = 16
    # signed = false
    # returns 16899
    #
    # ==== Examples
    #   # getInt(a) == 16899
    #   Exp.eq(BitExp.getInt(Exp.val(8), Exp.val(16), false, Exp.blobBin("a")), Exp.val(16899))
    def self.get_int(bit_offset, bit_size, signed, bin)
      bytes = self.pack_get_int(bit_offset, bit_size, signed)
      self.add_read(bin, bytes, Exp::Type::INT)
    end

    private

    MODULE = 1
    RESIZE = 0
    INSERT = 1
    REMOVE = 2
    SET = 3
    OR = 4
    XOR = 5
    AND = 6
    NOT = 7
    LSHIFT = 8
    RSHIFT = 9
    ADD = 10
    SUBTRACT = 11
    SET_INT = 12
    GET = 50
    COUNT = 51
    LSCAN = 52
    RSCAN = 53
    GET_INT = 54

    INT_FLAGS_SIGNED = 1

    def self.pack_math(command, policy, bit_offset, bit_size, value, signed, bit_overflow_action)
      Packer.use do |packer|
        # Pack.init only required when CTX is used and server does not support CTX for bit operations.
        # Pack.init(packer, ctx)
        packer.write_array_header(6)
        packer.write(command)
        bit_offset.pack(packer)
        bit_size.pack(packer)
        value.pack(packer)
        packer.write(policy.flags)

        flags = bit_overflow_action
        flags |= INT_FLAGS_SIGNED if signed

        packer.write(flags)
        return packer.bytes
      end
    end

    def self.pack_get_int(bit_offset, bit_size, signed)
      Packer.use do |packer|
        # Pack.init only required when CTX is used and server does not support CTX for bit operations.
        # Pack.init(packer, ctx)
        packer.write_array_header(signed ? 4 : 3)
        packer.write(GET_INT)
        bit_offset.pack(packer)
        bit_size.pack(packer)
        packer.write(INT_FLAGS_SIGNED) if signed
        return packer.bytes
      end
    end

    def self.add_write(bin, bytes)
      Exp::Module.new(bin, bytes, Exp::Type::BLOB, MODULE | Exp::MODIFY)
    end

    def self.add_read(bin, bytes, ret_type)
      Exp::Module.new(bin, bytes, ret_type, MODULE)
    end
  end # class
end # module
