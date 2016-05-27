# Copyright 2012-2014 Aerospike, Inc.#
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

require 'rubygems'
require 'aerospike'
require './shared/shared'

include Aerospike
include Shared

def main
  Shared.init
  run_example(Shared.client)

  Shared.logger.info("Example finished successfully.")
end

class Metrics
  attr_accessor :count, :total

  def initialize
    @count = 0
    @total = 0
  end
end

@set_map = {}

def run_example(client)
  Shared.logger.info("Scan series: namespace=#{Shared.namespace}, set=#{Shared.set_name}")

  # Use low scan priority.  This will take more time, but it will reduce
  # the load on the server.
  policy = ScanPolicy.new
  policy.max_retries = 1
  policy.priority = Priority::LOW

  node_list = client.nodes
  begin_time = Time.now

  node_list.each do |node|
    Shared.logger.info("Scan node #{node.name}")
    recordset = client.scan_node(node, Shared.namespace, Shared.set_name, [], policy)

    recordset.each do |rec|
      metrics = @set_map[rec.key.set_name]
      metrics ||= Metrics.new

      metrics.count+=1
      metrics.total+=1
      @set_map[rec.key.set_name] = metrics
    end

    @set_map.each do |k, v|
      Shared.logger.info("Node #{node}, set #{k}, count: #{v.count}")
      v.count = 0
    end
  end

  end_time = Time.now
  seconds = end_time - begin_time
  Shared.logger.info("Elapsed time: #{seconds} seconds")

  total = 0

  @set_map.each do |k, v|
    Shared.logger.info("Total set #{k}, count: #{v.total}")
    total += v.total
  end

  Shared.logger.info("Grand total: #{total}")
  performance = (total.to_f/seconds.to_f).round(2)
  Shared.logger.info("Records/second: #{performance}")
end

main