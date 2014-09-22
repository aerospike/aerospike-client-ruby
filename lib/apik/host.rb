# Copyright 2012-2014 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

module Apik

  class Host
    include Optionable

    attr_accessor :name, :port

    # option(:name).any(String)
    # option(:port.any(Int)

    def initialize(hostName, hostPort)
      @name = hostName
      @port = hostPort
    end

    def to_s
      "#{@name}:#{@port.to_s}"
    end

    def ==(other)
      other && other.is_a?(Host) && other.name == @name && other.port == @port
    end

  end

end
