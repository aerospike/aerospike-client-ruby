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

require 'thread'
require 'time'

require 'aerospike/task/task'

module Aerospike

  protected

  class UdfRemoveTask < Task

    def initialize(cluster, package_name)
      super(cluster, false)
      @package_name = package_name

      self
    end

    private

    def all_nodes_done?
      command = 'udf-list'
      nodes = @cluster.nodes
      done = false

      nodes.each do |node|
        conn = node.get_connection(1)
        response_map = Info.request(conn, command)
        _, response = response_map.first
        index = response.index("filename=#{@package_name}")

        return false if index

        @done.value = true
        @done_event.broadcast
      end

      return done
    end

  end # class

end # module
