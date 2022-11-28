# encoding: utf-8
# Copyright 2016-2020 Aerospike, Inc.
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

require "msgpack"
require "aerospike/utils/pool"

module Aerospike
  class Packer < MessagePack::Packer #:nodoc:
    AS_EXT_TYPE = -1

    @@pool = Pool.new
    @@pool.create_proc = lambda do
      Packer.new.tap do |p|
        p.register_type(AS_EXT_TYPE, Aerospike::WildcardValue, :to_msgpack_ext)
        p.register_type(AS_EXT_TYPE, Aerospike::InfinityValue, :to_msgpack_ext)
      end
    end

    def self.use
      packer = @@pool.poll
      packer.clear
      yield packer
    ensure
      @@pool.offer(packer)
    end

    # WARNING: This method is not compatible with message pack standard.
    def write_raw_short(val)
      buffer << [val].pack("S>")
    end

    def write_raw(buf)
      buffer.write(buf)
    end

    def bytes
      self.to_s.force_encoding("binary")
    end
  end
end
