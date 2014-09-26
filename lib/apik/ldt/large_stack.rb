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

  class LargeStack < Large

    def initialize(client, policy, key, binName, userModule=nil)
      @PACKAGE_NAME = 'lstack'

      super(client, policy, key, binName, userModule)

      self
    end

    # Push values onto stack.  If the stack does not exist, create it using specified userModule configuration.
    #
    # values      values to push
    def push(*values)
      if values.length == 1
        @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'push', @binName, values[0], @userModule)
      else
        @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'push_all', @binName, values, @userModule)
      end
    end

    # Select items from top of stack.
    #
    # peekCount     number of items to select.
    # returns          list of items selected
    def peek(peekCount)
      @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'peek', @binName, peekCount)
    end

    # Select items from top of stack.
    #
    # peekCount     number of items to select.
    # returns          list of items selected
    def pop(count)
      @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'pop', @binName, count)
    end

    # Select items from top of stack.
    #
    # peekCount     number of items to select.
    # filterName    Lua function name which applies filter to returned list
    # filterArgs    arguments to Lua function name
    # returns          list of items selected
    def filter(peekCount, filterName, *filterArgs)
      @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'filter', @binName, peekCount, @userModule, filterName, filterArgs)
    end

  end # class

end #class
