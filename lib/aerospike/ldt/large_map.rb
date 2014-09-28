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

require 'aerospike/ldt/large'

module Aerospike

  class LargeMap < Large

    def initialize(client, policy, key, bin_name, user_module=nil)
      @PACKAGE_NAME = 'lmap'

      super(client, policy, key, bin_name, user_module)

      self
    end

    # Add entry to map.  If the map does not exist, create it using specified user_module configuration.
    #
    # name        entry key
    # value       entry value
    def put(name, value)
      @client.execute_udf(@key, @PACKAGE_NAME, 'put', [@bin_name, name, value, @user_module], @policy)
    end

    # Add map values to map.  If the map does not exist, create it using specified user_module configuration.
    #
    # map       map values to push
    def put_map(the_map)
      @client.execute_udf(@key, @PACKAGE_NAME, 'put_all', [@bin_name, the_map, @user_module], @policy)
    end

    # Get value from map given name key.
    #
    # name        key.
    # return          map of items selected
    def get(name)
      begin
        @client.execute_udf(@key, @PACKAGE_NAME, 'get', [@bin_name, name, @user_module], @policy)
      rescue Aerospike::Exceptions::Aerospike => e
        unless e.result_code == Aerospike::ResultCode::UDF_BAD_RESPONSE && e.message.index("Item Not Found")
          raise e
        end
        nil
      end
    end

    # Get value from map given name key.
    #
    # name        key.
    # return          map of items selected
    def remove(name)
      @client.execute_udf(@key, @PACKAGE_NAME, 'remove', [@bin_name, name, @user_module], @policy)
    end

    # Select items from map.
    #
    # filter_name    Lua function name which applies filter to returned list
    # filter_args    arguments to Lua function name
    # return          list of items selected
    def filter(filter_name, *filter_args)
      @client.execute_udf(@key, @PACKAGE_NAME, 'filter', [@bin_name, @user_module, filter_name, filter_args], @policy)
    end

  end # class

end #class
