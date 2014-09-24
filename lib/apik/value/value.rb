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

require 'apik/aerospike_exception'
require 'apik/value/particle_type'

module Apik

  # Polymorphic value classes used to efficiently serialize objects into the wire protocol.
  class Value

    def self.of(value)
      case value
      when nil
        res = NullValue.new
      when Integer
        if value < 2**31
          res = IntegerValue.new(value)
        elsif value < 2**63
          res = LongValue.new(value)
        else
          # big nums > 2**63 are not supported
          raise Apik::Exceptions::Aerospike.new(TYPE_NOT_SUPPORTED, "Value type #{value.class} not supported.").freeze
        end
      when String
        res = StringValue.new(value)
      when Value
        res = value
      when Map
        res = MapValue.new(value)
      when Array
        res = ListValue.new(value)
      when AerospikeBlob
        res = BlobValue.new(value)
      else
        # panic for anything that is not supported.
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


    def estimateSize
      0
    end

    def write(buffer, offset)
      0
    end

    def pack(packer)
      packer.PackNil
    end

    # GetLuaValue LuaValue {
    #  LuaNil.NIL
    # }

    def to_bytes
      nil
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
      Buffer.BytesToHexString(@bytes)
    end

    # def GetLuaValue LuaValue {
    #  LuaString.valueOf(@bytes)
    # end

    def to_bytes
      @bytes
    end

    def estimateSize
      @bytes.bytesize
    end

    def write(buffer, offset)
      buffer.write_binary(bytes, offset)
      buffer.length
    end

    def pack(packer)
      packer.PackBytes(@bytes)
    end

  end # BytesValue

  #######################################

  # value string.
  class StringValue < Value

    def initialize(val)
      @value = val
      self
    end

    def estimateSize
      @value.bytesize
    end

    def write(buffer, offset)
      buffer.write_binary(@value, offset)
    end

    def pack(packer)
      packer.PackString(@value)
    end

    def type
      Apik::ParticleType::STRING
    end

    def get
      @value
    end

    # def GetLuaValue LuaValue {
    #  LuaString.valueOf(@value)
    # }

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
      @value = val
      self
    end

    def estimateSize
      8
    end

    def write(buffer, offset)
      buffer.write_int64(@value, offset)
      8
    end

    def pack(packer)
      packer.PackAInt(@value)
    end

    def type
      Apik::ParticleType::INTEGER
    end

    def get
      @value
    end

    # def GetLuaValue LuaValue {
    #  LuaInteger.valueOf(@value)
    # }

    def to_bytes
      [@value].pack('l<'.freeze)
    end

    def to_s
      @value.to_s
    end

  end # IntegerValue

  #######################################/

  # Long value.
  class LongValue < Value

    def NewLongValue(value)
      @value = val

      self
    end

    def type
      Apik::ParticleType::INTEGER
    end

    def get
      @value
    end

    def to_s
      @value.to_s
    end


    # def GetLuaValue LuaValue {
    #  LuaInteger.valueOf(@value)
    # }

    def to_bytes
      [@value].pack('q<'.freeze)
    end

    def estimateSize
      8
    end

    def write(buffer, offset)
      buffer.write_int64(@value, offset)
      8
    end

    def pack(packer)
      packer.PackALong(value)
    end

  end # LongValue

  #######################################/

  # # Value array.
  # # Supported by Aerospike 3 servers only.
  # type ValueArray < Value
  #   array []Value
  #   bytes []byte
  # end

  # def ToValueArray(array []interface{}) *ValueArray {
  #   res = make([]Value, 0, len(array))
  #   for i = range array {
  #     res = append(res, NewValue(array[i]))
  #   end
  #   NewValueArray(res)
  # end

  # def NewValueArray(array []Value) *ValueArray {
  #   res = &ValueArray{
  #     array: array,
  #   end

  #   res.bytes, _ = packValueArray(array)

  #   res
  # end

  # def estimateSize
  #   len(@bytes)
  # end

  # def
  #   res = copy(buffer[offset:], @bytes)
  #   res, nil
  # end

  # def
  #   packer.packValueArray(@array)
  # end

  # def type
  #   Apik::ParticleType::LIST
  # end

  # def get
  #   @array
  # end

  # # def GetLuaValue LuaValue {
  # #  nil
  # # }

  # def to_bytes
  #   @bytes
  # end

  # def to_s
  #   fmt.Sprintf("%v", @array)
  # end

  # #######################################/

  # # List value.
  # # Supported by Aerospike 3 servers only.
  # type ListValue < Value
  #   list  []interface{}
  #   bytes []byte
  # end

  # def NewListValue(list []interface{}) *ListValue {
  #   res = &ListValue{
  #     list: list,
  #   end

  #   res.bytes, _ = packAnyArray(list)

  #   res
  # end

  # def estimateSize
  #   # var err error
  #   @bytes, _ = packAnyArray(@list)
  #   len(@bytes)
  # end

  # def
  #   l = copy(buffer[offset:], @bytes)
  #   l, nil
  # end

  # def
  #   packer.PackList(@list)
  # end

  # def type
  #   Apik::ParticleType::LIST
  # end

  # def get
  #   @list
  # end

  # # def GetLuaValue LuaValue {
  # #  nil
  # # }

  # def to_bytes
  #   @bytes
  # end

  # def to_s
  #   fmt.Sprintf("%v", @list)
  # end

  # #######################################/

  # # Map value.
  # # Supported by Aerospike 3 servers only.
  # type MapValue < Value
  #   vmap  map[interface{}]interface{}
  #   bytes []byte
  # end

  # def NewMapValue(vmap map[interface{}]interface{}) *MapValue {
  #   res = &MapValue{
  #     vmap: vmap,
  #   end

  #   res.bytes, _ = packAnyMap(vmap)

  #   res
  # end

  # def estimateSize
  #   len(@bytes)
  # end

  # def
  #   copy(buffer[offset:], @bytes), nil
  # end

  # def
  #   packer.PackMap(@vmap)
  # end

  # def type
  #   Apik::ParticleType::MAP
  # end

  # def get
  #   @vmap
  # end

  # # def GetLuaValue LuaValue {
  # #  nil
  # # }

  # def to_bytes
  #   @bytes
  # end

  # def to_s
  #   fmt.Sprintf("%v", @vmap)
  # end

  #######################################

  def self.bytesToParticle(type, buf , offset, length)

    case type
    when Apik::ParticleType::STRING
      StringValue.new(buf.read(offset, length))

    when Apik::ParticleType::INTEGER
      Value.of(buf.read_int64(offset))

    when Apik::ParticleType::BLOB
      BytesValue.new(buf.read(offse,length))

    # when Apik::ParticleType::LIST
    #   newUnpacker(buf, offset, length).UnpackList

    # when Apik::ParticleType::MAP
    #   newUnpacker(buf, offset, length).UnpackMap

    else
      nil
    end
  end

  def self.bytesToKeyValue(type, buf, offset, len)

    case type
    when Apik::ParticleType::STRING
      StringValue.new(buf.read(offset, len))

    when Apik::ParticleType::INTEGER
      LongValue.new(buf.read_var_int64(offset, len))

    when Apik::ParticleType::BLOB
      BytesValue.new(buf.read(offset,len))

    else
      nil
    end
  end

end # module
