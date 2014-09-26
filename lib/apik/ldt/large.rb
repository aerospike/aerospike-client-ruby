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

require 'apik/value/particle_type'

module Apik

  class Large

    def initialize(client, policy, key, binName, userModule=nil)
      @client = client
      @policy = policy
      @key = key
      @binName = Apik::ParticleType::STRING.chr + binName
      @userModule = Apik::ParticleType::STRING.chr + userModule unless userModule.nil?

      self
    end

    # Delete bin containing the object.
    def destroy
      @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'destroy', @binName)
    end

    # Return size of object.
    def size
      @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'size', @binName)
    end

    # Return map of object configuration parameters.
    def config
      @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'get_config', @binName)
    end

    # Set maximum number of entries in the object.
    #
    # capacity      max entries in set
    def capacity=(capacity)
      @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'set_capacity', @binName, capacity)
    end

    # Return maximum number of entries in the object.
    def capacity
      @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'get_capacity', @binName)
    end

    # Return list of all objects on the stack.
    def scan
      @client.execute_udf(@policy, @key, @PACKAGE_NAME, 'scan', @binName)
    end

  end # class

end #class
