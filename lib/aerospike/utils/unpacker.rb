# encoding: utf-8
# Copyright 2016-2018 Aerospike, Inc.
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

module Aerospike

  private

  class Unpacker

    @@pool = Pool.new
    @@pool.create_proc = Proc.new { Unpacker.new }

    def self.use
      unpacker = @@pool.poll
      unpacker.reset
      yield unpacker
    ensure
      @@pool.offer(unpacker)
    end

    MsgPackExt = Struct.new(:type, :data)
    MsgPackExt::TYPES = [
             # Map Create Flags:         List Create Flags:
      0x00,  # UNORDERED                 UNORDERED
      0x01,  # K_ORDERED                 ORDERED
      0x03,  # KV_ORDERED
      0x08,  # PRESERVE_ORDER
    ]

    def initialize
      @unpacker = MessagePack::Unpacker.new
      MsgPackExt::TYPES.each do |type|
        @unpacker.register_type(type) { |data| MsgPackExt.new(type, data) }
      end
    end

    def unpack(bytes)
      obj = @unpacker.feed(bytes).read
      case obj
      when Array then unpack_list(obj)
      when Hash  then unpack_map(obj)
      else obj
      end
    end

    def reset
      @unpacker.reset
    end

    private

    def unpack_list(array)
      list = normalize_strings_in_array(array)
      unless list.empty?
        list.shift if MsgPackExt === list.first
      end
      list
    end

    def unpack_map(hash)
      hash = normalize_strings_in_map(hash)
      unless hash.empty?
        (key, _) = hash.first
        hash.shift if MsgPackExt === key
      end
      hash
    end

    def normalize_elem(elem)
      case elem
      when String
        ptype = elem.ord
        value = elem[1..-1]
        if (ptype == ParticleType::STRING)
          value.encode!(Aerospike.encoding)
        end
        value
      when Array
        normalize_strings_in_array(elem)
      when Hash
        normalize_strings_in_map(elem)
      else
        elem
      end
    end

    def normalize_strings_in_array(arr)
      arr.map! { |elem| normalize_elem(elem) }
    end

    def normalize_strings_in_map(hash)
      hash.inject({}) do |h, (k,v)|
        h.update({ normalize_elem(k) => normalize_elem(v) })
      end
    end

  end

end
