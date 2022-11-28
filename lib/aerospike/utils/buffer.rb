# encoding: utf-8

# Copyright 2014-2020 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require "aerospike/utils/pool"

module Aerospike
  private

  # Buffer class to ease the work around
  class Buffer #:nodoc:
    @@buf_pool = Pool.new
    @@buf_pool.create_proc = Proc.new { Buffer.new }

    attr_accessor :buf

    INT16 = "s>"
    UINT16 = "n"
    UINT16LE = "v"
    INT32 = "l>"
    UINT32 = "N"
    INT64 = "q>"
    UINT64 = "Q>"
    UINT64LE = "Q"
    DOUBLE = "G"

    DEFAULT_BUFFER_SIZE = 16 * 1024
    MAX_BUFFER_SIZE = 10 * 1024 * 1024

    def initialize(size = DEFAULT_BUFFER_SIZE, buf = nil)
      @buf = (buf ? buf : ("%0#{size}d" % 0))
      @buf.force_encoding("binary")
      @slice_end = @buf.bytesize
    end

    def self.get
      @@buf_pool.poll
    end

    def self.put(buffer)
      @@buf_pool.offer(buffer)
    end

    def size
      @buf.bytesize
    end

    alias_method :length, :size

    def eat!(n)
      @buf.replace(@buf[n..-1])
    end

    def resize(length)
      # Corrupted data streams can result in a hug.length.
      # Do a sanity check here.
      if length > MAX_BUFFER_SIZE
        raise Aerospike::Exceptions::Parse.new("Invalid size for buffer: #{length}")
      end

      if @buf.bytesize < length
        @buf.concat("%0#{length - @buf.bytesize}d" % 0)
      end
      @slice_end = length
    end

    def write_byte(byte, offset)
      @buf.setbyte(offset, byte.ord)
      1
    end

    def write_binary(data, offset)
      @buf[offset, data.bytesize] = data
      data.bytesize
    end

    def write_int16(i, offset)
      @buf[offset, 2] = [i].pack(INT16)
      2
    end

    def write_uint16(i, offset)
      @buf[offset, 2] = [i].pack(UINT16)
      2
    end

    def write_uint16_little_endian(i, offset)
      @buf[offset, 2] = [i].pack(UINT16LE)
      2
    end

    def write_int32(i, offset)
      @buf[offset, 4] = [i].pack(INT32)
      4
    end

    def write_uint32(i, offset)
      @buf[offset, 4] = [i].pack(UINT32)
      4
    end

    def write_int64(i, offset)
      @buf[offset, 8] = [i].pack(INT64)
      8
    end

    def write_uint64(i, offset)
      @buf[offset, 8] = [i].pack(UINT64)
      8
    end

    def write_uint64_little_endian(i, offset)
      @buf[offset, 8] = [i].pack(UINT64LE)
      8
    end

    def write_double(f, offset)
      @buf[offset, 8] = [f].pack(DOUBLE)
      8
    end

    def read(offset, len = nil)
      if len
        @buf[offset, len]
      else
        @buf.getbyte(offset)
      end
    end

    def read_int16(offset)
      vals = @buf[offset..offset + 1]
      vals.unpack(INT16)[0]
    end

    def read_uint16(offset)
      vals = @buf[offset..offset + 1]
      vals.unpack(UINT16)[0]
    end

    def read_int32(offset)
      vals = @buf[offset..offset + 3]
      vals.unpack(INT32)[0]
    end

    def read_uint32(offset)
      vals = @buf[offset..offset + 3]
      vals.unpack(UINT32)[0]
    end

    def read_int64(offset)
      vals = @buf[offset..offset + 7]
      vals.unpack(INT64)[0]
    end

    def read_uint64_little_endian(offset)
      vals = @buf[offset..offset + 7]
      vals.unpack(UINT64LE)[0]
    end

    def read_uint64(offset)
      vals = @buf[offset..offset + 7]
      vals.unpack(UINT64)[0]
    end

    def read_var_int64(offset, len)
      val = 0
      i = 0
      while i < len
        val <<= 8
        val |= @buf[offset + i].ord & 0xFF
        i = i.succ
      end
      val
    end

    def read_double(offset)
      vals = @buf[offset..offset + 7]
      vals.unpack(DOUBLE)[0]
    end

    def read_bool(offset, length)
      length <= 0 ? false : @buf[offset].ord != 0
    end

    def to_s
      @buf[0..@slice_end - 1]
    end

    def reset
      for i in 0..@buf.size - 1
        @buf[i] = " "
      end
    end

    def dump(start = 0, finish = nil)
      buf ||= @buf.bytes
      finish ||= @slice_end - 1
      width = 16

      ascii = "|"
      counter = 0

      print "%08x  " % start
      @buf.bytes[start...finish].each do |c|
        if counter >= start
          print "%02x " % c
          ascii << (c.between?(32, 126) ? c : ?.)
          print " " if ascii.length == (width / 2 + 1)
          if ascii.length > width
            ascii << "|"
            puts ascii
            ascii = "|"
            print "%08x  " % (counter + 1)
          end
        end
        counter += 1
      end

      # print the remainder in buffer
      if ascii.length.positive?
        fill_size = ((width - ascii.length + 1) * 3)
        fill_size += 1 if ascii.length <= (width / 2)
        filler = " " * fill_size
        print filler
        ascii << "|"
        puts ascii
      end
    end
  end # buffer
end # module
