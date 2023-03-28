# encoding: utf-8
# Copyright 2014-2020 Aerospike, Inc.
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

  private

  class IndexTask < Task

    MATCHER = /.*load_pct=(?<load_pct>\d+(\.\d+)?).*/.freeze

    def initialize(cluster, namespace, index_name, done=false)
      super(cluster, done)
      @namespace = namespace
      @index_name = index_name

      self
    end

    private

    def all_nodes_done?
      command = "sindex/#{@namespace}/#{@index_name}"
      nodes = @cluster.nodes

      nodes.each do |node|
        begin
          conn = node.get_connection(1)
        rescue => e
          Aerospike.logger.error("Get connection failed with exception: #{e}")
          raise e
        end
        response_map = Info.request(conn, command)
        _, response = response_map.first
        match = response.to_s.match(MATCHER)
        load = match.nil? ? nil : match[:load_pct]

        return false if load && (0...100).include?(load.to_f)
      end

      return true
    end

  end # class

end # module
