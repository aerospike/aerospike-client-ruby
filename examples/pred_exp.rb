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
  run_mapkey_predexp_example(Shared.client)
  run_list_predexp_example(Shared.client)

  Shared.logger.info("Example finished successfully.")
end

def setup(client)
  records = 100
  Shared.logger.info("Creating #{records} records with random properties.")
  records.times do |idx|
    key = Key.new(Shared.namespace, Shared.set_name, "user#{idx}")
    record = {
      'name' => %w[Timmy Alice John Arthur Mike Diana Emily Laura Nicole].sample + "_#{idx}",
      'race' => %w[Inkling Octoling].sample,
      'level' => rand(1..100),
      'rank' => ['C-', 'C', 'C+', 'B-', 'B', 'B+', 'A-', 'A', 'A+', 'S', 'S+', 'X'].sample,
      'gear' => {
        'headgear' => ['18K Aviators', 'Designer Headphones', 'Knitted Hat', 'Long-Billed Hat', 'Pilot Goggles'].sample,
        'clothes' => ['Takoroka Windcrusher', 'White Tee', 'Takoroka Jersey', 'Pearl Tee', 'King Jersey', 'Chilly Mountain Coat'].sample,
        'shoes' => ['Blue Sea Slugs', 'Green Laceups', 'N-Pacer Ag', 'N-Pacer Au', 'Neon Sea Slugs', 'Trail Boots'].sample
      },
      'weapons' => ['Splattershot', 'Splatling', 'Slosher', 'Splat Charger', 'Splat Dualies', 'N-ZAP', 'Jet Squelcher', 'Splat Roller', 'Kensa Roller'].sample(3),
      'loc' => GeoJSON.new(type: 'Point', coordinates: [(3 + (idx * 0.003)), (4 + (idx * 0.003))])
    }
    client.put(key, record)
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
  Shared.logger.info("Querying set using predicate expressions to return all users with level > 30")

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

  Shared.logger.info("Found #{results.length} records with level > 30: #{results}")
end

def run_string_predexp_example(client)
  Shared.logger.info("Querying set using predicate expressions to return all Inklings")

  statement = Statement.new(Shared.namespace, Shared.set_name)
  statement.predexp = [
    PredExp.string_bin('race'),
    PredExp.string_value('Inkling'),
    PredExp.string_equal
  ]

  records = client.query(statement)
  results = []
  records.each do |r|
    results << r.bins['name']
  end

  Shared.logger.info("Found #{results.length} Inklings: #{results}")
end

def run_regex_predexp_example(client)
  Shared.logger.info("Querying set using predicate expressions to return all B rank users")

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

  Shared.logger.info("Found #{results.length} users with B rank: #{results}")
end

def run_mapkey_predexp_example(client)
  Shared.logger.info("Querying set using predicate expressions to return all users wearing 18K Aviators")

  statement = Statement.new(Shared.namespace, Shared.set_name)
  statement.predexp = [
    PredExp.string_value('18K Aviators'),
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

  Shared.logger.info("Found #{results.length} users wearing 18K Aviators: #{results}")
end

def run_list_predexp_example(client)
  Shared.logger.info("Querying set using predicate expressions to return all users using Sloshers")

  statement = Statement.new(Shared.namespace, Shared.set_name)
  statement.predexp = [
    PredExp.string_value('Slosher'),
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

  Shared.logger.info("Found #{results.length} users using Sloshers: #{results}")
end
