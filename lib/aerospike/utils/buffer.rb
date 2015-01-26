# encoding: utf-8
# Copyright 2014 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require 'aerospike/utils/pool'

module Aerospike

  private

  # Buffer class to ease the work around
  class Buffer #:nodoc:

    attr_reader :buf

    @@buf_pool = Pool.new
    @@buf_pool.create_block = Proc.new { Buffer.new }

    attr_accessor :buf

    INT16 = 's>'
    UINT16 = 'n'
    INT32 = 'l>'
    UINT32 = 'N'
    INT64 = 'q>'
    UINT64 = 'Q>'

    DEFAULT_BUFFER_SIZE = 16 * 1024
    MAX_BUFFER_SIZE = 10 * 1024 * 1024

    def initialize(size=DEFAULT_BUFFER_SIZE)
      @buf = "%0#{size}d" % 0

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

    def resize(length)
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

    def read(offset, len=nil)
      start = offset

      if len
        @buf[start, len]
      else
        @buf.getbyte(start)
      end
    end

    def read_int16(offset)
      vals = @buf[offset..offset+1]
      vals.unpack(INT16)[0]
    end

    def read_int32(offset)
      vals = @buf[offset..offset+3]
      vals.unpack(INT32)[0]
    end

    def read_int64(offset)
      vals = @buf[offset..offset+7]
      vals.unpack(INT64)[0]
    end

    def read_var_int64(offset, len)
      val = 0
      i = 0
      while i < len
        val <<= 8
        val |= @buf[offset+i].ord & 0xFF
        i = i.succ
      end
      val
    end

    def to_s
      @buf[0..@slice_end-1]
    end

    def dump(from=nil, to=nil)
      from ||= 0
      to ||= @slice_end - 1

      @buf.bytes[from...to].each do |c|
        print c.ord.to_s(16)
        putc ' '
      end
    end

  end # buffer

end # module
