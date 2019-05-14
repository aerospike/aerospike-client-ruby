# frozen_string_literal: true

require "rubygems"
require "aerospike"
require './shared/shared'

include Aerospike
include Shared

def main
  Shared.init
  setup(Shared.client)
  teardown(Shared.client)

  run_integer_predexp_example(Shared.client)
  run_string_predexp_example(Shared.client)
  run_regex_predexp_example(Shared.client)
  run_mapval_predexp_example(Shared.client)
  run_list_predexp_example(Shared.client)
  run_geojson_predexp_example(Shared.client)
  run_void_time_predexp_example(Shared.client)

  Shared.logger.info("Example finished successfully.")
end

def setup(client)
  records = 100
  Shared.logger.info("Creating #{records} records with random properties.")
  records.times do |idx|
    key = Key.new(Shared.namespace, Shared.set_name, "user#{idx}")
    record = {
      'name' => %w[Timmy Alice John Arthur Mike Diana Emily Laura Nicole].sample + "_#{idx}",
      'race' => %w[Squid Octopus].sample,
      'level' => rand(1..100),
      'rank' => ['C-', 'C', 'C+', 'B-', 'B', 'B+', 'A-', 'A', 'A+', 'S', 'S+', 'X'].sample,
      'gear' => {
        'clothes' => ['Green Hoodie', 'White Tee', 'Blue Jersey', 'Black Tee', 'Mountain Coat'].sample
      },
      'weapons' => ['Water Gun', 'Paint Roller', 'Paintbrush', 'Aerospray', 'Bucket'].sample(3),
      'loc' => GeoJSON.new(type: 'Point', coordinates: [(3 + (idx * 0.003)), (4 + (idx * 0.003))])
    }
    client.put(key, record, ttl: (idx + 1) * 5)
  end

  task = client.create_index(Shared.namespace, Shared.set_name, "name_index", "name", :string)
  task.wait_till_completed or fail "Could not create secondary 'name' index"

  task = client.create_index(Shared.namespace, Shared.set_name, "level_index", "level", :numeric)
  task.wait_till_completed or fail "Could not create secondary 'level' index"

  task = client.create_index(Shared.namespace, Shared.set_name, "loc_index", "loc", :geo2dsphere)
  task.wait_till_completed or fail "Could not create secondary 'loc' index"
end

def teardown(client)
  client.drop_index(Shared.namespace, Shared.set_name, "name_index")
  client.drop_index(Shared.namespace, Shared.set_name, "level_index")
  client.drop_index(Shared.namespace, Shared.set_name, "loc_index")
end

def run_integer_predexp_example(client)
  Shared.logger.info("Querying set using predicate expressions to return users with level > 30")

  statement = Statement.new(Shared.namespace, Shared.set_name)
  statement.predexp = [
    PredExp.integer_bin('level'),
    PredExp.integer_value(30),
    PredExp.integer_greater
  ]

  records = client.query(statement)
  results = []
  records.each do |r|
    results << r.bins['name']
  end

  Shared.logger.info("Found #{results.length} records with level > 30.")
end

def run_string_predexp_example(client)
  Shared.logger.info("Querying set using predicate expressions to return Squids")

  statement = Statement.new(Shared.namespace, Shared.set_name)
  statement.predexp = [
    PredExp.string_bin('race'),
    PredExp.string_value('Squid'),
    PredExp.string_equal
  ]

  records = client.query(statement)
  results = []
  records.each do |r|
    results << r.bins['name']
  end

  Shared.logger.info("Found #{results.length} Squids.")
end

def run_regex_predexp_example(client)
  Shared.logger.info("Querying set using predicate expressions to return B rank users")

  statement = Statement.new(Shared.namespace, Shared.set_name)
  statement.predexp = [
    PredExp.string_bin('rank'),
    PredExp.string_value('B'),
    PredExp.string_regex(PredExp::RegexFlags::NONE)
  ]

  records = client.query(statement)
  results = []
  records.each do |r|
    results << r.bins['name']
  end

  Shared.logger.info("Found #{results.length} users with B rank.")
end

def run_mapval_predexp_example(client)
  Shared.logger.info("Querying set using predicate expressions to return all users wearing White Tees")

  statement = Statement.new(Shared.namespace, Shared.set_name)
  statement.predexp = [
    PredExp.string_value('White Tee'),
    PredExp.string_var('x'),
    PredExp.string_equal,
    PredExp.map_bin('gear'),
    PredExp.mapval_iterate_or('x')
  ]

  records = client.query(statement)
  results = []
  records.each do |r|
    results << r.bins['name']
  end

  Shared.logger.info("Found #{results.length} users wearing White Tees.")
end

def run_list_predexp_example(client)
  Shared.logger.info("Querying set using predicate expressions to return users using buckets")

  statement = Statement.new(Shared.namespace, Shared.set_name)
  statement.predexp = [
    PredExp.string_value('Bucket'),
    PredExp.string_var('x'),
    PredExp.string_equal,
    PredExp.list_bin('weapons'),
    PredExp.list_iterate_or('x')
  ]

  records = client.query(statement)
  results = []
  records.each do |r|
    results << r.bins['name']
  end

  Shared.logger.info("Found #{results.length} users using buckets.")
end

def run_geojson_predexp_example(client)
  Shared.logger.info("Querying set using predicate expressions to return users in range of circle")

  circle_range = 1_000
  # circle with range of 1000 meters
  circle = GeoJSON.new(type: 'AeroCircle', coordinates: [[3,4], circle_range])

  statement = Statement.new(Shared.namespace, Shared.set_name)
  statement.predexp = [
    PredExp.geojson_bin('loc'),
    PredExp.geojson_value(circle),
    PredExp.geojson_contains
  ]

  records = client.query(statement)
  results = []
  records.each do |r|
    results << r.bins['name']
  end

  Shared.logger.info("Found #{results.length} users in a circle.")
end

def run_void_time_predexp_example(client)
  Shared.logger.info("Querying set using predicate expressions to return records expiring in less than a minute")

  minute_from_now = Time.now + 60
  # Provided time must be an Epoch in nanoseconds
  minute_from_now = ("%10.9f" % minute_from_now.to_f).gsub('.', '').to_i

  statement = Statement.new(Shared.namespace, Shared.set_name)
  statement.predexp = [
    Aerospike::PredExp.integer_value(minute_from_now),
    Aerospike::PredExp.void_time,
    Aerospike::PredExp.integer_greater
  ]

  records = client.query(statement)
  results = []
  records.each do |r|
    results << r.bins['name']
  end

  Shared.logger.info("Found #{results.length} records expiring in less than a minute.")
end

main
