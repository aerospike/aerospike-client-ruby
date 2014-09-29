# encoding: utf-8
# Copyright 2014 Aerospike, Inc.
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

module Aerospike

  class Key

    @@digest_pool = Pool.new
    @@digest_pool.create_block = Proc.new do
      unless RUBY_PLATFORM == 'java'
        Digest::RMD160.new
      else
        h = OpenSSL::Digest::RIPEMD160.new
      end
    end


    attr_reader :namespace, :set_name, :digest

    def initialize(ns, set, val, digest=nil)
      @namespace = ns
      @set_name = set
      @user_key = Value.of(val)

      unless digest
        compute_digest
      else
        @digest = digest
      end

      self
    end

    def to_s
      "#{@namespace}:#{@set_name}:#{@user_key}"
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

    private

    def compute_digest
      # get a hash from pool and make it ready for work
      h = @@digest_pool.poll
      h.reset

      # Compute a complete digest
      h.update(@set_name)
      h.update(@user_key.to_bytes)
      @digest = h.digest

      # put the hash object back to the pool
      @@digest_pool.offer(h)
    end

  end

end
