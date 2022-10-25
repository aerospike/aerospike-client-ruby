# encoding: utf-8
# Copyright 2014-2020 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require 'digest'

require 'aerospike/value/value'
require 'aerospike/utils/pool'

module Aerospike

  class Key

    @@digest_pool = Pool.new
    @@digest_pool.create_proc = Proc.new do
      if RUBY_PLATFORM == 'java'
        OpenSSL::Digest::RIPEMD160.new
      else
        Digest(:RMD160).new
      end
    end

    # enable backwards compatibility with v1 client for integer keys
    # ref. https://github.com/aerospike/aerospike-client-ruby/pull/34
    def self.enable_v1_compatibility!(comp = true)
      @v1_compatibility = !!comp
    end
    def self.v1_compatible?
      @v1_compatibility
    end

    # Keys other than integers, strings and bytes are unsupported and will trigger a warning if used.
    # Starting with v3 the client will raise an error instead of a warning.
    # ref. https://github.com/aerospike/aerospike-client-ruby/issues/43
    def self.disable_unsupported_key_warning!(enable_warning = false)
      @unsupported_key_warning = enable_warning
    end
    def self.warn_unsupported_key?
      @unsupported_key_warning
    end

    attr_reader :namespace, :set_name, :digest

    def initialize(ns, set, val, digest=nil, bval: nil, v1_compatible: self.class.v1_compatible?)
      @namespace = ns
      @set_name = set
      @user_key = Value.of(val)
      check_key!(@namespace, @set_name, @user_key, !digest.nil?)
      @digest = digest || compute_digest(v1_compatible)
      @bval = bval
    end


    def bval
      @bval
    end

    def to_s
      "#{@namespace}:#{@set_name}:#{@user_key}:#{@digest.nil? ? '' : @digest.bytes}"
    end

    def user_key
      @user_key.get if @user_key
    end

    def user_key_as_value
      @user_key
    end

    def ==(other)
      other && other.is_a?(Key) &&
        other.digest == @digest &&
        other.namespace == @namespace
    end
    alias eql? ==

    def hash
      @digest.hash
    end

    def partition_id
      (@digest[0..3].unpack(Partition::UNPACK_FORMAT)[0] & 0xFFFF) % Node::PARTITIONS
    end

    private

    def valid_key?(value, has_digest)
      value.is_a?(IntegerValue) ||
        value.is_a?(StringValue) ||
        value.is_a?(BytesValue) ||
        (has_digest && value.is_a?(NullValue))
    end

    def check_key!(ns, set, value, has_digest)
      if self.class.warn_unsupported_key? && !valid_key?(value, has_digest)
        warn("Unsupported key type: #{value.class.name} - only Integer, String and Bytes are supported")
      end
    end

    def compute_digest(v1_compatible = false)
      key_type = @user_key.type
      key_bytes = @user_key.to_bytes

      if key_type == Aerospike::ParticleType::NULL
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::PARAMETER_ERROR, "Invalid key: nil")
      end

      # v1.0.12 and prior computed integer key digest using little endian byte order
      if key_type == Aerospike::ParticleType::INTEGER && v1_compatible
        key_bytes.reverse!
      end

      # get a hash from pool and make it ready for work
      h = @@digest_pool.poll
      h.reset

      # Compute a complete digest
      h.update(@set_name)
      h.update(key_type.chr)
      h.update(key_bytes)
      digest = h.digest

      # put the hash object back to the pool
      @@digest_pool.offer(h)

      digest
    end

  end

end
