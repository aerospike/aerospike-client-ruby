# Copyright 2015-2017 Aerospike, Inc.#
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
  run_points_with_region_example(Shared.client)
  run_regions_containing_point_example(Shared.client)
  teardown(Shared.client)

  Shared.logger.info("Example finished successfully.")
end

def setup(client)
  task = client.create_index(Shared.namespace, Shared.set_name, "geospatial_index", "location", :geo2dsphere)
  task.wait_till_completed or fail "Could not create geospatial index"
end

def teardown(client)
  client.drop_index(Shared.namespace, Shared.set_name, "geospatial_index")
end

def run_points_with_region_example(client)
  points_to_create = 20
  orig = [103.9114, 1.3083]
  Shared.logger.info("Creating #{points_to_create} records with random locations.")
  points_to_create.times do |i|
    key = Key.new(Shared.namespace, Shared.set_name, "geo#{i}")
    offsets = Array.new(2){ Random.rand - 0.5 }
    coords = [(orig[0] + offsets[0]).round(4), (orig[1] + offsets[1]).round(4)]
    point = GeoJSON.new({type: "Point", coordinates: coords})
    Shared.logger.info("#{key.user_key}: #{point}")
    client.put(key, "location" => point)
  end

  region = GeoJSON.new({type: "Polygon", coordinates: [[[103.6055, 1.1587], [103.6055, 1.4707], [104.0884, 1.4707], [104.0884, 1.1587], [103.6055, 1.1587]]]})
  Shared.logger.info("Querying set using 'geoWithinGeoJSONRegion' filter for all records representing points within GeoJSON region: #{region}")
  stmt = Statement.new(Shared.namespace, Shared.set_name)
  stmt.filters << Filter.geoWithinGeoJSONRegion("location", region)
  records = client.query(stmt)

  matching_keys = []
  records.each do |record|
    matching_keys << record.key.user_key
  end
  Shared.logger.info("Matching records with location within region: #{matching_keys.join(", ")}")
end

def run_regions_containing_point_example(client)
  regions_to_create = 20
  orig = [[103.6055, 1.1587], [103.6055, 1.4707], [104.0884, 1.4707], [104.0884, 1.1587], [103.6055, 1.1587]]
  Shared.logger.info("Creating #{regions_to_create} records with random polygon geographies.")
  regions_to_create.times do |i|
    key = Key.new(Shared.namespace, Shared.set_name, "geo#{i}")
    offsets = Array.new(2){ Random.rand - 0.5 }
    coords = orig.map{|(x, y)| [(x + offsets[0]).round(4), (y + offsets[1]).round(4)]}
    region = GeoJSON.new({type: "Polygon", coordinates: [coords]})
    Shared.logger.info("#{key.user_key}: #{region}")
    client.put(key, "location" => region)
  end

  point = GeoJSON.new({type: "Point", coordinates: [103.9114, 1.3083]})
  Shared.logger.info("Querying set using 'geoContainsGeoJSONPoint' filter for all records representing regions which contain GeoJSON point: #{point}")
  stmt = Statement.new(Shared.namespace, Shared.set_name)
  stmt.filters << Filter.geoContainsGeoJSONPoint("location", point)
  records = client.query(stmt)

  matching_keys = []
  records.each do |record|
    matching_keys << record.key.user_key
  end
  Shared.logger.info("Matching records with location within region: #{matching_keys.join(", ")}")
end

main
