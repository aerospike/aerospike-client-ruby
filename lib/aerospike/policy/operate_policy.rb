# encoding: utf-8
# Copyright 2016-2017 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'aerospike/policy/write_policy'
require 'aerospike/policy/record_bin_multiplicity'

module Aerospike

  class OperatePolicy < WritePolicy

    attr_accessor :record_bin_multiplicity

    def initialize(opt = {})
      super(opt)

      # Specifies how to merge results from multiple operations returning
      # results for the same record bin.
      @record_bin_multiplicity = opt[:record_bin_multiplicity] || RecordBinMultiplicity::SINGLE
    end

  end

end
