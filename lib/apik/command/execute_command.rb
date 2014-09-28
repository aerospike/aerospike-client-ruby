# encoding: utf-8
# Copyright 2014 Aerospike, Inc.
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
require 'apik/command/read_command'

module Apik

  protected

  class ExecuteCommand < ReadCommand

    def initialize(cluster, policy, key, package_name, function_name, args)
      super(cluster, policy, key, nil)

      @package_name = package_name
      @function_name = function_name
      @args = ListValue.new(args)

      self
    end

    def write_buffer
      set_udf(@key, @package_name, @function_name, @args)
    end

  end # class

end # module
