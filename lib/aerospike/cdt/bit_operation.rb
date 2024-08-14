# frozen_string_literal: true

# Copyright 2016-2020 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

module Aerospike
  module CDT

    ##
    # Bit operations. Create bit operations used by client operate command.
    # Offset orientation is left-to-right.  Negative offsets are supported.
    # If the offset is negative, the offset starts backwards from end of the bitmap.
    # If an offset is out of bounds, a parameter error will be returned.
    #
    # Nested CDT operations are supported by optional context arguments.  Example:
    # bin = [[0b00000001, 0b01000010],[0b01011010]]
    # Resize first bitmap (in a list of bitmaps) to 3 bytes.
    # BitOperation.resize("bin", 3, BitResizeFlags::DEFAULT, ctx: [Context.list_index(0)])
    # bin result = [[0b00000001, 0b01000010, 0b00000000],[0b01011010]]
    class BitOperation < Operation

      RESIZE   = 0
      INSERT   = 1
      REMOVE   = 2
      SET      = 3
      OR       = 4
      XOR      = 5
      AND      = 6
      NOT      = 7
      LSHIFT   = 8
      RSHIFT   = 9
      ADD      = 10
      SUBTRACT = 11
      SET_INT  = 12
      GET      = 50
      COUNT    = 51
      LSCAN    = 52
      RSCAN    = 53
      GET_INT  = 54

      INT_FLAGS_SIGNED = 1

      attr_reader :bit_op, :arguments, :policy, :ctx

      def initialize(op_type, bit_op, bin_name, *arguments, ctx: nil, policy: nil)
        @op_type = op_type
        @bin_name = bin_name
        @bin_value = nil
        @bit_op = bit_op
        @ctx = ctx
        @arguments = arguments
      end

      # BitResizeOp creates byte "resize" operation.
      # Server resizes byte[] to byte_size according to resize_flags (See {BitResizeFlags}).
      # Server does not return a value.
      # Example:
      # bin = [0b00000001, 0b01000010]
      # byte_size = 4
      # resize_flags = 0
      # bin result = [0b00000001, 0b01000010, 0b00000000, 0b00000000]
      def self.resize(bin_name, byte_size, resize_flags, ctx: nil, policy: BitPolicy::DEFAULT)
        BitOperation.new(Operation::BIT_MODIFY, RESIZE, bin_name, byte_size, policy.flags, resize_flags, ctx: ctx, policy: policy)
      end

      # BitInsertOp creates byte "insert" operation.
      # Server inserts value bytes into byte[] bin at byte_offset.
      # Server does not return a value.
      # Example:
      # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
      # byte_offset = 1
      # value = [0b11111111, 0b11000111]
      # bin result = [0b00000001, 0b11111111, 0b11000111, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
      def self.insert(bin_name, byte_offset, value, ctx: nil, policy: BitPolicy::DEFAULT)
        BitOperation.new(Operation::BIT_MODIFY, INSERT, bin_name, byte_offset, value_to_bytes(value), policy.flags, ctx: ctx, policy: policy)
      end

      # BitRemoveOp creates byte "remove" operation.
      # Server removes bytes from byte[] bin at byte_offset for byte_size.
      # Server does not return a value.
      # Example:
      # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
      # byte_offset = 2
      # byte_size = 3
      # bin result = [0b00000001, 0b01000010]
      def self.remove(bin_name, byte_offset, byte_size, ctx: nil, policy: BitPolicy::DEFAULT)
        BitOperation.new(Operation::BIT_MODIFY, REMOVE, bin_name, byte_offset, byte_size, policy.flags, ctx: ctx, policy: policy)
      end

      # BitSetOp creates bit "set" operation.
      # Server sets value on byte[] bin at bit_offset for bit_size.
      # Server does not return a value.
      # Example:
      # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
      # bit_offset = 13
      # bit_size = 3
      # value = [0b11100000]
      # bin result = [0b00000001, 0b01000111, 0b00000011, 0b00000100, 0b00000101]
      def self.set(bin_name, bit_offset, bit_size, value, ctx: nil, policy: BitPolicy::DEFAULT)
        BitOperation.new(Operation::BIT_MODIFY, SET, bin_name, bit_offset, bit_size, value_to_bytes(value), policy.flags, ctx: ctx, policy: policy)
      end

      # BitOrOp creates bit "or" operation.
      # Server performs bitwise "or" on value and byte[] bin at bit_offset for bit_size.
      # Server does not return a value.
      # Example:
      # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
      # bit_offset = 17
      # bit_size = 6
      # value = [0b10101000]
      # bin result = [0b00000001, 0b01000010, 0b01010111, 0b00000100, 0b00000101]
      def self.or(bin_name, bit_offset, bit_size, value, ctx: nil, policy: BitPolicy::DEFAULT)
        BitOperation.new(Operation::BIT_MODIFY, OR, bin_name, bit_offset, bit_size, value_to_bytes(value), policy.flags, ctx: ctx, policy: policy)
      end

      # BitXorOp creates bit "exclusive or" operation.
      # Server performs bitwise "xor" on value and byte[] bin at bit_offset for bit_size.
      # Server does not return a value.
      # Example:
      # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
      # bit_offset = 17
      # bit_size = 6
      # value = [0b10101100]
      # bin result = [0b00000001, 0b01000010, 0b01010101, 0b00000100, 0b00000101]
      def self.xor(bin_name, bit_offset, bit_size, value, ctx: nil, policy: BitPolicy::DEFAULT)
        BitOperation.new(Operation::BIT_MODIFY, XOR, bin_name, bit_offset, bit_size, value_to_bytes(value), policy.flags, ctx: ctx, policy: policy)
      end

      # BitAndOp creates bit "and" operation.
      # Server performs bitwise "and" on value and byte[] bin at bit_offset for bit_size.
      # Server does not return a value.
      # Example:
      # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
      # bit_offset = 23
      # bit_size = 9
      # value = [0b00111100, 0b10000000]
      # bin result = [0b00000001, 0b01000010, 0b00000010, 0b00000000, 0b00000101]
      def self.and(bin_name, bit_offset, bit_size, value, ctx: nil, policy: BitPolicy::DEFAULT)
        BitOperation.new(Operation::BIT_MODIFY, AND, bin_name, bit_offset, bit_size, value_to_bytes(value), policy.flags, ctx: ctx, policy: policy)
      end

      # BitNotOp creates bit "not" operation.
      # Server negates byte[] bin starting at bit_offset for bit_size.
      # Server does not return a value.
      # Example:
      # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
      # bit_offset = 25
      # bit_size = 6
      # bin result = [0b00000001, 0b01000010, 0b00000011, 0b01111010, 0b00000101]
      def self.not(bin_name, bit_offset, bit_size, ctx: nil, policy: BitPolicy::DEFAULT)
        BitOperation.new(Operation::BIT_MODIFY, NOT, bin_name, bit_offset, bit_size, policy.flags, ctx: ctx, policy: policy)
      end

      # BitLShiftOp creates bit "left shift" operation.
      # Server shifts left byte[] bin starting at bit_offset for bit_size.
      # Server does not return a value.
      # Example:
      # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
      # bit_offset = 32
      # bit_size = 8
      # shift = 3
      # bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00101000]
      def self.lshift(bin_name, bit_offset, bit_size, shift, ctx: nil, policy: BitPolicy::DEFAULT)
        BitOperation.new(Operation::BIT_MODIFY, LSHIFT, bin_name, bit_offset, bit_size, shift, policy.flags, ctx: ctx, policy: policy)
      end

      # BitRShiftOp creates bit "right shift" operation.
      # Server shifts right byte[] bin starting at bit_offset for bit_size.
      # Server does not return a value.
      # Example:
      # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
      # bit_offset = 0
      # bit_size = 9
      # shift = 1
      # bin result = [0b00000000, 0b11000010, 0b00000011, 0b00000100, 0b00000101]
      def self.rshift(bin_name, bit_offset, bit_size, shift, ctx: nil, policy: BitPolicy::DEFAULT)
        BitOperation.new(Operation::BIT_MODIFY, RSHIFT, bin_name, bit_offset, bit_size, shift, policy.flags, ctx: ctx, policy: policy)
      end

      # BitAddOp creates bit "add" operation.
      # Server adds value to byte[] bin starting at bit_offset for bit_size. Bit_size must be <= 64.
      # Signed indicates if bits should be treated as a signed number.
      # If add overflows/underflows, {BitOverflowAction} is used.
      # Server does not return a value.
      # Example:
      # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
      # bit_offset = 24
      # bit_size = 16
      # value = 128
      # signed = false
      # bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b10000101]
      def self.add(
        bin_name,
        bit_offset,
        bit_size,
        value,
        signed,
        action,
        ctx: nil,
        policy: BitPolicy::DEFAULT
      )
        actionFlags = action
        actionFlags |= INT_FLAGS_SIGNED if signed

        BitOperation.new(Operation::BIT_MODIFY, ADD, bin_name, bit_offset, bit_size, value, policy.flags, actionFlags, ctx: ctx, policy: policy)
      end

      # BitSubtractOp creates bit "subtract" operation.
      # Server subtracts value from byte[] bin starting at bit_offset for bit_size. Bit_size must be <= 64.
      # Signed indicates if bits should be treated as a signed number.
      # If add overflows/underflows, {BitOverflowAction} is used.
      # Server does not return a value.
      # Example:
      # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
      # bit_offset = 24
      # bit_size = 16
      # value = 128
      # signed = false
      # bin result = [0b00000001, 0b01000010, 0b00000011, 0b0000011, 0b10000101]
      def self.subtract(
        bin_name,
        bit_offset,
        bit_size,
        value,
        signed,
        action,
        ctx: nil,
        policy: BitPolicy::DEFAULT
      )
        actionFlags = action
        actionFlags |= INT_FLAGS_SIGNED if signed

        BitOperation.new(Operation::BIT_MODIFY, SUBTRACT, bin_name, bit_offset, bit_size, value, policy.flags, actionFlags, ctx: ctx, policy: policy)
      end

      # BitSetIntOp creates bit "setInt" operation.
      # Server sets value to byte[] bin starting at bit_offset for bit_size. Size must be <= 64.
      # Server does not return a value.
      # Example:
      # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
      # bit_offset = 1
      # bit_size = 8
      # value = 127
      # bin result = [0b00111111, 0b11000010, 0b00000011, 0b0000100, 0b00000101]
      def self.set_int(bin_name, bit_offset, bit_size, value, ctx: nil, policy: BitPolicy::DEFAULT)
        BitOperation.new(Operation::BIT_MODIFY, SET_INT, bin_name, bit_offset, bit_size, value, policy.flags, ctx: ctx, policy: policy)
      end

      # BitGetOp creates bit "get" operation.
      # Server returns bits from byte[] bin starting at bit_offset for bit_size.
      # Example:
      # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
      # bit_offset = 9
      # bit_size = 5
      # returns [0b1000000]
      def self.get(bin_name, bit_offset, bit_size, ctx: nil)
        BitOperation.new(Operation::BIT_READ, GET, bin_name, bit_offset, bit_size, ctx: ctx)
      end

      # BitCountOp creates bit "count" operation.
      # Server returns integer count of set bits from byte[] bin starting at bit_offset for bit_size.
      # Example:
      # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
      # bit_offset = 20
      # bit_size = 4
      # returns 2
      def self.count(bin_name, bit_offset, bit_size, ctx: nil)
        BitOperation.new(Operation::BIT_READ, COUNT, bin_name, bit_offset, bit_size, ctx: ctx)
      end

      # BitLScanOp creates bit "left scan" operation.
      # Server returns integer bit offset of the first specified value bit in byte[] bin
      # starting at bit_offset for bit_size.
      # Example:
      # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
      # bit_offset = 24
      # bit_size = 8
      # value = true
      # returns 5
      def self.lscan(bin_name, bit_offset, bit_size, value, ctx: nil)
        BitOperation.new(Operation::BIT_READ, LSCAN, bin_name, bit_offset, bit_size, value && true, ctx: ctx)
      end

      # BitRScanOp creates bit "right scan" operation.
      # Server returns integer bit offset of the last specified value bit in byte[] bin
      # starting at bit_offset for bit_size.
      # Example:
      # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
      # bit_offset = 32
      # bit_size = 8
      # value = true
      # returns 7
      def self.rscan(bin_name, bit_offset, bit_size, value, ctx: nil)
        BitOperation.new(Operation::BIT_READ, RSCAN, bin_name, bit_offset, bit_size, value && true, ctx: ctx)
      end

      # BitGetIntOp creates bit "get integer" operation.
      # Server returns integer from byte[] bin starting at bit_offset for bit_size.
      # Signed indicates if bits should be treated as a signed number.
      # Example:
      # bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
      # bit_offset = 8
      # bit_size = 16
      # signed = false
      # returns 16899
      def self.get_int(bin_name, bit_offset, bit_size, signed, ctx: nil)
        if signed
          BitOperation.new(Operation::BIT_READ, GET_INT, bin_name, bit_offset, bit_size, INT_FLAGS_SIGNED, ctx: ctx)
        else
          BitOperation.new(Operation::BIT_READ, GET_INT, bin_name, bit_offset, bit_size, ctx: ctx)
        end
      end

      def bin_value
        @bin_value ||= pack_bin_value
      end

      private

      def self.value_to_bytes(value)
        case value
        when Integer
          [value].pack('C*')
        when Array
          value.pack('C*')
        when String
          BytesValue.new(value)
        when StringValue
          BytesValue.new(value.get)
        else
          value
        end
      end

      def pack_bin_value
        bytes = nil
        args = arguments.dup
        Packer.use do |packer|
          if !@ctx.nil? && @ctx.length > 0
            packer.write_array_header(3)
            Value.of(0xff).pack(packer)

            packer.write_array_header(@ctx.length*2)
            @ctx.each do |ctx|
              Value.of(ctx.id).pack(packer)
              Value.of(ctx.value, true).pack(packer)
            end
          end

          packer.write_array_header(args.length+1)
          Value.of(@bit_op, true).pack(packer)
          args.each do |value|
            Value.of(value, true).pack(packer)
          end
          bytes = packer.bytes
        end
        BytesValue.new(bytes)
      end
    end
  end
end
