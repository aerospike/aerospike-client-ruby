# encoding: utf-8
# Copyright 2014-2022 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may no
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

module Aerospike
  ##
  # Expression operations.
  class Exp::Operation
    ##
    # Create operation that performs an expression that writes to a record bin.
    # Requires server version 5.6.0+.
    #
    # @param bin_name	name of bin to store expression result
    # @param exp		expression to evaluate
    # @param flags		expression write flags.  See {Exp::WriteFlags}
    def self.write(bin_name, exp, flags = Aerospike::Exp::WriteFlags::DEFAULT)
      create_operation(Aerospike::Operation::EXP_MODIFY, bin_name, exp, flags)
    end

    ##
    # Create operation that performs a read expression.
    # Requires server version 5.6.0+.
    #
    # @param name		variable name of read expression result. This name can be used as the
    # 					bin name when retrieving bin results from the record.
    # @param exp		expression to evaluate
    # @param flags		expression read flags.  See {Exp::ExpReadFlags}
    def self.read(name, exp, flags = Aerospike::Exp::ReadFlags::DEFAULT)
      create_operation(Aerospike::Operation::EXP_READ, name, exp, flags)
    end

    private

    def self.create_operation(type, name, exp, flags)
      Packer.use do |packer|
        packer.write_array_header(2)
        exp.pack(packer)
        packer.write(flags)

        return Operation.new(type, name, BytesValue.new(packer.bytes))
      end
    end
  end
end
