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

require 'aerospike/key'

module Support

  RAND_CHARS = ('a'..'z').to_a.concat(('A'..'Z').to_a).concat(('0'..'9').to_a)
  VERSION_REGEX = /\d+(?:.\d+)+(:?-\d+)?(?:-[a-z0-9]{8})?/

  def self.rand_string(len)
    RAND_CHARS.shuffle[0,len].join
  end

  def self.gen_random_key(len=50, opts = {set: 'test'})
    key_val = rand_string(len)
    set_name = opts[:set] || 'test'
    Aerospike::Key.new('test', set_name, key_val)
  end

  def self.delete_set(client, namespace, set_name)
    if min_version?("3.12.0")
      return client.truncate(namespace, set_name)
    end

    package = "test_utils_delete_record.lua"
    function = <<EOF
function delete_record(record)
  aerospike:remove(record)
end
EOF
    register_task = client.register_udf(function, package, Aerospike::Language::LUA)
    register_task.wait_till_completed or fail "Could not register delete_record UDF to delete set #{set_name}"
    statement = Aerospike::Statement.new(namespace, set_name)
    execute_task = client.execute_udf_on_query(statement, package, "delete_record")
    execute_task.wait_till_completed
    remove_task = client.remove_udf(package)
    remove_task.wait_till_completed or fail "Could not un-register delete_record UDF to delete set #{set_name}"
  end

  def self.client
    @client ||= begin
      host = Aerospike::Host.new('0.0.0.0', '3000')
      policy = Aerospike::ClientPolicy.new(
        user: ENV['AEROSPIKE_USER'],
        password: ENV['AEROSPIKE_PASSWORD'],
      )
      Aerospike::Client.new(host, policy: policy)
    end
  end

  def self.feature?(feature)
    self.client.supports_feature?(feature.to_s)
  end

  def self.enterprise?
    @enterprise_edition ||=
      begin
        info = self.client.request_info("edition")
        info["edition"] =~ /Enterprise/
      end
  end

  def self.version
    @cluster_version ||=
      begin
        version = ENV['AEROSPIKE_VERSION_OVERRIDE']
        version ||= self.client.request_info("version")["version"]
        version = version[VERSION_REGEX]
        Gem::Version.new(version).release
      end
  end

  # returns true if the server runs at least the specified minimum version
  # of ASD (e.g. "3.9.1")
  def self.min_version?(version)
    version = Gem::Version.new(version)
    self.version >= version
  end

  def self.tls_supported?
    # Skip TLS specs on JRuby until this issue is resolved:
    # https://github.com/jruby/jruby-openssl/issues/172
    !is_jruby?
  end

  def self.is_jruby?
    RUBY_PLATFORM == "java"
  end

  module Geo
    RAD_PER_DEG        = Math::PI / 180.to_f
    DEG_PER_RAD        = 180.to_f / Math::PI
    EARTH_RADIUS_IN_KM = 6371.to_f
    EARTH_RADIUS_IN_M  = EARTH_RADIUS_IN_KM * 1000;

    #----------------------------------------------------------------------------
    ##
    ## Radians to degrees
    ##
    def self.rad2deg(rad)
      rad.to_f * DEG_PER_RAD
    end

    #----------------------------------------------------------------------------
    ##
    ## Degrees to radians
    ##
    def self.deg2rad(deg)
      deg.to_f * RAD_PER_DEG
    end

    #----------------------------------------------------------------------------
    ##
    ## Calculate coordinates of a point located
    ## on a set distance and bearing from start point
    ##
    ## @param [Float] lng longitude of starting point
    ## @param [Float] lat latitude of starting point
    ## @param [Integer] distance distance in meters
    ## @param [Integer] bearing
    ##
    ## @return [Aerospike::GeoJSON] Point in a set distance and bearing
    ##
    def self.destination_point(lng, lat, distance, bearing)
      ang_dis = (distance.to_f / EARTH_RADIUS_IN_M) # Earth's radius in meters

      br = deg2rad(bearing)
      rlat = deg2rad(lat)
      rlng = deg2rad(lng)

      # Some math magic

      f_lat = Math.asin((Math.sin(rlat) * Math.cos(ang_dis)) + (Math.cos(rlat) * Math.sin(ang_dis) * Math.cos(br)))
      f_lng = rlng + Math.atan2((Math.sin(br) * Math.sin(ang_dis) * Math.cos(rlat)), Math.cos(ang_dis) - (Math.sin(rlat) * Math.sin(f_lat)))

      # And convert it back to degrees

      f_lat = rad2deg(f_lat)
      f_lng = rad2deg(f_lng)

      Aerospike::GeoJSON.new(type: 'Point', coordinates: [f_lng, f_lat])
    end

    # Create a random Point in distance of set amount of meters
    #
    ## @param [Float] lng longitude of starting point
    ## @param [Float] lat latitude of starting point
    # @param distance [Integer] Distance in meters
    # @return [Aerospike::Point] Random point in distance within _distance_ meters
    def self.random_point_in_range(lng, lat, distance)
      destination_point(
        lng,
        lat,
        rand(1..distance),
        rand(0..360)
      )
    end
  end

end
