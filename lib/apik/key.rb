# Copyright 2012-2014 Aerospike, Inc.
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

require 'apik/value/value'

module Apik

  class Key

    attr_reader :namespace, :setName, :digest

    def initialize(ns, set, val, digest=nil)
      @namespace = ns
      @setName = set
      @userKey = Value.of(val)

      unless digest
        compute_digest
      else
        @digest = digest
      end

      self
    end

    def to_s
      @namespace + ':' + @setName + ':' + @userKey.to_s
    end

    def userKey
      @userKey.get if @userKey
    end

    def userKeyAsValue
      @userKey
    end

    def ==(other)
      other && other.is_a?(Key) && other.namespace == @namespace && other.setName == @setName && other.userKey == @userKey
    end

    def digest_to_intel_int
      ((@digest.byteslice(3).ord & 0xFF) << 24) |
      ((@digest.byteslice(2).ord & 0xFF) << 16) |
      ((@digest.byteslice(1).ord & 0xFF) << 8) |
      (@digest.byteslice(0).ord & 0xFF)
    end

    private

    def compute_digest
      # Compute a complete digest
      h = Digest::RMD160.new
      h.update(@setName)
      h.update(@userKey.to_bytes)
      @digest = h.digest
    end

  end

end
