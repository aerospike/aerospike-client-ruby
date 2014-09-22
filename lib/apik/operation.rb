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

require 'apik/value/value'

module Apik

  class Operation

    attr_reader :op_type, :bin_name, :bin_value

    READ        = 1
    READ_HEADER = 1
    WRITE       = 2
    ADD         = 5
    APPEND      = 9
    PREPEND     = 10
    TOUCH       = 11

    def initialize(op_type, bin_name=nil, bin_value=NullValue.new)
      @op_type = op_type
      @bin_name = @bin_name
      @bin_value = @bin_value
      self
    end

    def self.get(bin_name=nil)
      Operation.new(READ, bin_name)
    end

    def self.get_header(bin_name=nil)
      Operation.new(READ_HEADER, bin_name)
    end

    def self.put(bin)
      Operation.new(PUT, bin.name, bin.value)
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

  end

end # module
