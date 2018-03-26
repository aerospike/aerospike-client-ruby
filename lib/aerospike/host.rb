# encoding: utf-8
# Copyright 2014-2018 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

module Aerospike

  class Host

    attr_accessor :name, :port, :tls_name

    def initialize(host_name, host_port, tls_name = nil)
      @name = host_name
      @port = host_port
      @tls_name = tls_name
    end

    def to_s
      "#{@name}:#{@port}"
    end
    alias_method :inspect, :to_s

    def ==(other)
      other && other.is_a?(Host) && other.name == @name && other.port == @port
    end
    alias eql? ==

    def hash
      to_s.hash
    end

  end

end
