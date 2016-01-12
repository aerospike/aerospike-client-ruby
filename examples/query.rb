# Copyright 2016 Aerospike, Inc.#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License") you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require "rubygems"
require "aerospike"
require './shared/shared'

include Aerospike
include Shared

def main
  Shared.init
  setup(Shared.client)
  run_query_using_equal_filter_example(Shared.client)
  run_query_using_range_filter_example(Shared.client)
  teardown(Shared.client)

  Shared.logger.info("Example finished successfully.")
end

def setup(client)
  records = 100
  Shared.logger.info("Creating #{records} records with random properties.")
  records.times do |idx|
    key = Key.new(Shared.namespace, Shared.set_name, "user#{idx}")
    record = {
      "name" => ["Fred", "Bob", "Laura", "Sammy", "Maude", "Pete", "Francisco", "Daniel", "Sarah", "Nora"].sample,
      "city" => ["Singapore", "Boston", "Hamburg", "San Francisco", "New Delhi", "Tokyo", "Sydney", "Montreal", "Istanbul", "Vientiane"].sample,
      "age" => rand(1..100)
    }
    client.put(key, record)
  end

  task = client.create_index(Shared.namespace, Shared.set_name, "name_index", "name", :string)
  task.wait_till_completed or fail "Could not create secondary 'name' index"

  task = client.create_index(Shared.namespace, Shared.set_name, "age_index", "age", :numeric)
  task.wait_till_completed or fail "Could not create secondary 'age' index"
end

def teardown(client)
  client.drop_index(Shared.namespace, Shared.set_name, "name_index")
  client.drop_index(Shared.namespace, Shared.set_name, "age_index")
end

def run_query_using_equal_filter_example(client)
  Shared.logger.info("Querying set using 'equal' filter for all records with name Bob")
  stmt = Statement.new(Shared.namespace, Shared.set_name, ["city"])
  stmt.filters << Filter.Equal("name", "Bob")
  records = client.query(stmt)

  results = []
  records.each do |record|
    results << record.bins["city"]
  end
  Shared.logger.info("Found #{results.length} records for 'Bob' in the following cities: #{results.join(", ")}")
end

def run_query_using_range_filter_example(client)
  Shared.logger.info("Querying set using 'range' filter for all records with age 20-50")
  stmt = Statement.new(Shared.namespace, Shared.set_name)
  stmt.filters << Filter.Range("age", 20, 50)
  records = client.query(stmt)

  results = []
  records.each do |record|
    results << "#{record.bins["name"]}, #{record.bins["age"]}, from #{record.bins["city"]}"
  end
  Shared.logger.info("Found #{results.length} results:\n#{results.join("\n")}")
end

main