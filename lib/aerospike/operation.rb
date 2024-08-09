# encoding: utf-8
# Copyright 2014-2020 Aerospike, Inc.
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

require "aerospike/value/value"

module Aerospike
  class Operation
    attr_reader :op_type, :bin_name, :bin_value, :ctx

    READ = 1
    READ_HEADER = 1
    WRITE = 2
    CDT_READ = 3
    CDT_MODIFY = 4
    ADD = 5
    EXP_READ = 7
    EXP_MODIFY = 8
    APPEND = 9
    PREPEND = 10
    TOUCH = 11
    BIT_READ = 12
    BIT_MODIFY = 13
    DELETE = 14
    HLL_READ = 15
    HLL_MODIFY = 16

    def initialize(op_type, bin_name = nil, bin_value = NullValue.new, ctx = nil)
      @op_type = op_type
      @bin_name = bin_name
      @bin_value = Value.of(bin_value)
      @ctx = ctx
      self
    end

    def bin
      Aerospike::Bin.new(bin_name, bin_value) if bin_name && bin_value
    end

    def self.get(bin_name = nil)
      Operation.new(READ, bin_name)
    end

    def self.get_header(bin_name = nil)
      Operation.new(READ_HEADER, bin_name)
    end

    def self.put(bin)
      Operation.new(WRITE, bin.name, bin.value)
    end

    def self.append(bin)
      Operation.new(APPEND, bin.name, bin.value)
    end

    def self.prepend(bin)
      Operation.new(PREPEND, bin.name, bin.value)
    end

    def self.add(bin)
      Operation.new(ADD, bin.name, bin.value)
    end

    def self.touch
      Operation.new(TOUCH)
    end

    def self.delete
      Operation.new(DELETE)
    end

    def is_write?
      case @op_type
      when READ
        false
      when READ_HEADER
        false
      when WRITE
        true
      when CDT_READ
        false
      when CDT_MODIFY
        true
      when ADD
        true
      when EXP_READ
        false
      when EXP_MODIFY
        true
      when APPEND
        true
      when PREPEND
        true
      when TOUCH
        true
      when BIT_READ
        false
      when BIT_MODIFY
        true
      when DELETE
        true
      when HLL_READ
        false
      when HLL_MODIFY
        true
      end
    end
  end
end # module
