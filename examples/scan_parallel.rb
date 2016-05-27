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

def run_example(client)
  Aerospike.logger = Shared.logger

  (0...10000).each do |i|
    Shared.client.put(Key.new(Shared.namespace, Shared.set_name, i), {"bin" => i})
  end

  Shared.logger.info("Scan parallel: namespace=#{Shared.namespace} set=#{Shared.set_name}")
  record_count = 0
  begin_time = Time.now

  policy = ScanPolicy.new
  recordset = client.scan_all(Shared.namespace, Shared.set_name, [], policy)

  recordset.each do |rec|
    record_count+=1
    Shared.logger.info("Records #{record_count}") if (record_count % 100) == 0
  end

  end_time = Time.now

  seconds = end_time - begin_time
  Shared.logger.info("Total records returned: #{record_count}")
  Shared.logger.info("Elapsed time: #{seconds} seconds")

  performance = (record_count.to_f/seconds.to_f).round(2)
  Shared.logger.info("Records/second: #{performance}")
end

main