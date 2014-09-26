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

  class LargeSet < Large

    def initialize(client, policy, key, binName, userModule=nil)
      @PACKAGE_NAME = 'lset'

      super(client, policy, key, binName, userModule)

      self
    end

    # Add values to the list.  If the list does not exist, create it using specified userModule configuration.
    #
    # values      values to add
    def add(*values)
      if values.length == 1
        @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'add', @binName, values[0], @userModule)
      else
        @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'add_all', @binName, values, @userModule)
      end
    end

    # Delete value from list.
    #
    # value       value to delete
    def remove(value)
      @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'remove', @binName, value)
    end

    # Select values from list.
    #
    # value       value to select
    # returns          list of entries selected
    def get(value)
      @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'get', @binName, value)
    end

    # Check existence of value in the set.
    #
    # value       value to check
    # returns          true if found, otherwise false
    def exists(value)
      1 == @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'exists', @binName, value)
    end

    # Select values from list and apply specified Lua filter.
    #
    # filterName    Lua function name which applies filter to returned list
    # filterArgs    arguments to Lua function name
    # returns          list of entries selected
    def filter(filterName, *filterArgs)
      @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'filter', @binName, @userModule, filterName, filterArgs)
    end

  end # class

end #class
