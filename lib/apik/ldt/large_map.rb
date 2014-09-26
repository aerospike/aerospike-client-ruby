# Copyright 2012-2014 Aerospike, Inc.
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

require 'apik/ldt/large'

module Apik

  class LargeMap < Large

    def initialize(client, policy, key, binName, userModule=nil)
      @PACKAGE_NAME = 'lmap'

      super(client, policy, key, binName, userModule)

      self
    end

    # Add entry to map.  If the map does not exist, create it using specified userModule configuration.
    #
    # name        entry key
    # value       entry value
    def put(name, value)
      @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'put', @binName, name, value, @userModule)
    end

    # Add map values to map.  If the map does not exist, create it using specified userModule configuration.
    #
    # map       map values to push
    def put_map(theMap)
      @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'put_all', @binName, theMap, @userModule)
    end

    # Get value from map given name key.
    #
    # name        key.
    # return          map of items selected
    def get(name)
      begin
        @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'get', @binName, name, @userModule)
      rescue Apik::Exceptions::Aerospike => e
        unless e.result_code == Apik::ResultCode::UDF_BAD_RESPONSE && e.message.index("Item Not Found")
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
      @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'remove', @binName, name, @userModule)
    end

    # Select items from map.
    #
    # filterName    Lua function name which applies filter to returned list
    # filterArgs    arguments to Lua function name
    # return          list of items selected
    def filter(filterName, *filterArgs)
      @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'filter', @binName, @userModule, filterName, filterArgs)
    end

  end # class

end #class
