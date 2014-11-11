# encoding: utf-8
# Copyright 2014 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the 'License'); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require 'aerospike/value/particle_type'

module Aerospike

  private

  class Large

    def initialize(client, policy, key, bin_name, user_module=nil)
      @client = client
      @policy = policy
      @key = key
      @bin_name = bin_name
      @user_module = user_module unless user_module.nil?

      self
    end

    # Delete bin containing the object.
    def destroy
      @client.execute_udf(@key, @PACKAGE_NAME, 'destroy', [@bin_name], @policy)
    end

    # Return size of object.
    def size
      @client.execute_udf(@key, @PACKAGE_NAME, 'size', [@bin_name], @policy)
    end

    # Return map of object configuration parameters.
    def config
      @client.execute_udf(@key, @PACKAGE_NAME, 'get_config', [@bin_name], @policy)
    end

    # Set maximum number of entries in the object.
    #
    # capacity      max entries in set
    def capacity=(capacity)
      @client.execute_udf(@key, @PACKAGE_NAME, 'set_capacity', [@bin_name, capacity], @policy)
    end

    # Return maximum number of entries in the object.
    def capacity
      @client.execute_udf(@key, @PACKAGE_NAME, 'get_capacity', [@bin_name], @policy)
    end

    # Return list of all objects on the stack.
    def scan
      @client.execute_udf(@key, @PACKAGE_NAME, 'scan', [@bin_name], @policy)
    end

  end # class

end #class
