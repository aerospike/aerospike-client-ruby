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

require 'msgpack'

require 'aerospike/utils/pool'

require 'aerospike/aerospike_exception'

module Aerospike

  private

  # Polymorphic value classes used to efficiently serialize objects into the wire protocol.
  class Value #:nodoc:

    @@packer_pool = Pool.new
    @@packer_pool.create_block = Proc.new { MessagePack::Packer.new }

    def self.get_packer
      res = @@packer_pool.poll
      res.clear
      res
    end

    def self.put_packer(packer)
      @@packer_pool.offer(packer)
    end

    def self.of(value)
      case value
      when nil
        res = NullValue.new
      when Integer
        if value < 2**63
          res = IntegerValue.new(value)
        else
          # big nums > 2**63 are not supported
          raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::TYPE_NOT_SUPPORTED, "Value type #{value.class} not supported.")
        end
      when String
        res = StringValue.new(value)
      when Value
        res = value
      when Hash
        res = MapValue.new(value)
      when Array
        res = ListValue.new(value)
      else
        # throw an exception for anything that is not supported.
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::TYPE_NOT_SUPPORTED, "Value type #{value.class} not supported.")
      end

      res
    end

  end # Value


  # Empty value.
  class NullValue < Value #:nodoc:

    def initialize
      self
    end

    def type
      Aerospike::ParticleType::NULL
    end

    def get
      nil
    end

    def to_s
      ''
    end


    def estimate_size
      0
    end

    def write(buffer, offset)
      0
    end

    def pack(packer)
      packer.write_nil
    end

    def to_bytes
      ''
    end
  end

  # Byte array value.
  class BytesValue < Value #:nodoc:

    def initialize(value)
      @bytes = value
      @bytes.force_encoding('binary')

      self
    end

    def type
      Aerospike::ParticleType::BLOB
    end

    def get
      @bytes
    end

    def to_s
      @bytes.to_s
    end

    def to_bytes
      @bytes.bytes
    end

    def estimate_size
      @bytes.bytesize
    end

    def write(buffer, offset)
      buffer.write_binary(@bytes, offset)
    end

    def pack(packer)
      packer.write(Aerospike::ParticleType::BLOB.chr + @bytes)
    end

  end # BytesValue

  #######################################

  # value string.
  class StringValue < Value #:nodoc:

    def initialize(val)
      @value = val || ''
      self
    end

    def estimate_size
      @value.bytesize
    end

    def write(buffer, offset)
      buffer.write_binary(@value, offset)
    end

    def pack(packer)
      packer.write(Aerospike::ParticleType::STRING.chr + @value)
    end

    def type
      Aerospike::ParticleType::STRING
    end

    def get
      @value
    end

    def to_bytes
      @value
    end

    def to_s
      @value
    end

  end # StringValue

  #######################################

  # Integer value.
  class IntegerValue < Value #:nodoc:

    def initialize(val)
      @value = val || 0
      self
    end

    def estimate_size
      8
    end

    def write(buffer, offset)
      buffer.write_int64(@value, offset)
      8
    end

    def pack(packer)
      packer.write(@value)
    end

    def type
      Aerospike::ParticleType::INTEGER
    end

    def get
      @value
    end

    def to_bytes
      [@value].pack('Q<')
    end

    def to_s
      @value.to_s
    end

  end # IntegerValue

  # List value.
  # Supported by Aerospike 3 servers only.
  class ListValue < Value #:nodoc:

    def initialize(list)
      @list = list || nil
      packer = Value.get_packer
      pack(packer)
      @bytes = packer.to_s.force_encoding('binary')
      Value.put_packer(packer)

      self
    end

    def estimate_size
      @bytes.bytesize
    end

    def write(buffer, offset)
      buffer.write_binary(@bytes, offset)
      @bytes.bytesize
    end

    def pack(packer)
      packer.write_array_header(@list.length)
      for val in @list
        Value.of(val).pack(packer)
      end
    end

    def type
      Aerospike::ParticleType::LIST
    end

    def get
      @list
    end

    def to_bytes
      @bytes
    end

    def to_s
      @list.map{|v| v.to_s}.to_s
    end

  end

  # #######################################/

  # Map value.
  # Supported by Aerospike 3 servers only.
  class MapValue < Value #:nodoc:

    def initialize(vmap)
      @vmap = vmap || {}

      packer = Value.get_packer
      pack(packer)
      @bytes = packer.to_s.force_encoding('binary')
      Value.put_packer(packer)

      self
    end

    def estimate_size
      @bytes.bytesize
    end

    def write(buffer, offset)
      buffer.write_binary(@bytes, offset)
      @bytes.bytesize
    end

    def pack(packer)
      packer.write_map_header(@vmap.length)
      # @vmap.each do |key, val|
      for key, val in @vmap
        Value.of(key).pack(packer)
        Value.of(val).pack(packer)
      end
    end

    def type
      Aerospike::ParticleType::MAP
    end

    def get
      @vmap
    end

    def to_bytes
      @bytes
    end

    def to_s
      @vmap.map{|k, v| "#{k.to_s} => #{v.to_s}" }.to_s
    end

  end

  #######################################

  protected

  def self.normalize_elem(elem) # :nodoc:
    case elem
    when String
     elem[1..-1]
    when Array
      normalize_strings_in_array(elem)
    when Hash
      normalize_strings_in_map(elem)
    else
      elem
    end
  end

  def self.normalize_strings_in_array(arr) # :nodoc:
    arr.map! { |elem| normalize_elem(elem) }
  end

  def self.normalize_strings_in_map(hash) # :nodoc:
    hash.inject({}) do |h, (k,v)|
      h.update({ normalize_elem(k) => normalize_elem(v) })
    end
  end

  def self.bytes_to_particle(type, buf, offset, length) # :nodoc:

    case type
    when Aerospike::ParticleType::STRING
      buf.read(offset, length)

    when Aerospike::ParticleType::INTEGER
      buf.read_int64(offset)

    when Aerospike::ParticleType::BLOB
      buf.read(offset,length)

    when Aerospike::ParticleType::LIST
      normalize_strings_in_array(MessagePack.unpack(buf.read(offset, length)))

    when Aerospike::ParticleType::MAP
      normalize_strings_in_map(MessagePack.unpack(buf.read(offset, length)))

    else
      nil
    end
  end

  def self.bytes_to_key_value(type, buf, offset, len) # :nodoc:

    case type
    when Aerospike::ParticleType::STRING
      StringValue.new(buf.read(offset, len))

    when Aerospike::ParticleType::INTEGER
      IntegerValue.new(buf.read_var_int64(offset, len))

    when Aerospike::ParticleType::BLOB
      BytesValue.new(buf.read(offset,len))

    else
      nil
    end
  end

end # module
