# encoding: utf-8
# Copyright 2016-2017 Aerospike, Inc.
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

  class Packer < MessagePack::Packer #:nodoc:

    @@pool = Pool.new
    @@pool.create_block = Proc.new { Packer.new }

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

    def bytes
      self.to_s.force_encoding('binary')
    end
  end

end
