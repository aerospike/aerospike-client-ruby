# Copyright 2012-2014 Aerospike, Inc.
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

require 'apik/utils/pool'

require 'apik/aerospike_exception'

module Apik

  # Polymorphic value classes used to efficiently serialize objects into the wire protocol.
  class Value

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
          raise Apik::Exceptions::Aerospike.new(TYPE_NOT_SUPPORTED, "Value type #{value.class} not supported.").freeze
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
        raise Apik::Exceptions::Aerospike.new(TYPE_NOT_SUPPORTED, "Value type #{value.class} not supported.").freeze
      end

      res
    end

  end # Value


  # Empty value.
  class NullValue < Value

    def initialize
      self
    end

    def type
      Apik::ParticleType::NULL
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
  class BytesValue < Value

    def initialize(value)
      @bytes = value
      @bytes.force_encoding('binary')

      self
    end

    def type
      Apik::ParticleType::BLOB
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
      buffer.write_binary(bytes, offset)
      buffer.length
    end

    def pack(packer)
      packer.write(@bytes.bytes)
    end

  end # BytesValue

  #######################################

  # value string.
  class StringValue < Value

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
      packer.write(@value)
    end

    def type
      Apik::ParticleType::STRING
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
  class IntegerValue < Value

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
      Apik::ParticleType::INTEGER
    end

    def get
      @value
    end

    def to_bytes
      [@value].pack('Q<'.freeze)
    end

    def to_s
      @value.to_s
    end

  end # IntegerValue

  # List value.
  # Supported by Aerospike 3 servers only.
  class ListValue < Value

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
      @list.each do |val|
        Value.of(val).pack(packer)
      end
    end

    def type
      Apik::ParticleType::LIST
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
  class MapValue < Value

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
      @vmap.each do |key, val|
        Value.of(key).pack(packer)
        Value.of(val).pack(packer)
      end
    end

    def type
      Apik::ParticleType::MAP
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

  def self.bytes_to_particle(type, buf , offset, length)

    case type
    when Apik::ParticleType::STRING
      StringValue.new(buf.read(offset, length))

    when Apik::ParticleType::INTEGER
      Value.of(buf.read_int64(offset))

    when Apik::ParticleType::BLOB
      BytesValue.new(buf.read(offse,length))

    when Apik::ParticleType::LIST
      ListValue.new(MessagePack.unpack(buf.read(offset, length)))

    when Apik::ParticleType::MAP
      MapValue.new(MessagePack.unpack(buf.read(offset, length)))

    else
      nil
    end
  end

  def self.bytes_to_key_value(type, buf, offset, len)

    case type
    when Apik::ParticleType::STRING
      StringValue.new(buf.read(offset, len))

    when Apik::ParticleType::INTEGER
      IntegerValue.new(buf.read_var_int64(offset, len))

    when Apik::ParticleType::BLOB
      BytesValue.new(buf.read(offset,len))

    else
      nil
    end
  end

end # module
