# encoding: utf-8
# Copyright 2014-2017 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

module Aerospike

  class Filter

    def self.Equal(bin_name, value)
      Filter.new(bin_name, value, value)
    end

    def self.Contains(bin_name, value, col_type)
      Filter.new(bin_name, value, value, nil, col_type)
    end

    def self.Range(bin_name, from, to, col_type = nil)
      Filter.new(bin_name, from, to, nil, col_type)
    end

    def self.geoWithinGeoJSONRegion(bin_name, region, col_type = nil)
      region = region.to_json
      Filter.new(bin_name, region, region, ParticleType::GEOJSON, col_type)
    end

    def self.geoWithinRadius(bin_name, lon, lat, radius_meter, col_type = nil)
      region = GeoJSON.new({type: "AeroCircle", coordinates: [[lon, lat], radius_meter]})
      geoWithinGeoJSONRegion(bin_name, region, col_type)
    end

    def self.geoContainsGeoJSONPoint(bin_name, point, col_type = nil)
      point = point.to_json
      Filter.new(bin_name, point, point, ParticleType::GEOJSON, col_type)
    end

    def self.geoContainsPoint(bin_name, lon, lat, col_type = nil)
      point = GeoJSON.new({type: "Point", coordinates: [lon, lat]})
      geoContainsGeoJSONPoint(bin_name, point, col_type)
    end

    def estimate_size
      return @name.bytesize + @begin.estimate_size + @end.estimate_size + 10
    end

    def write(buf, offset)
      # Write name.
      len = buf.write_binary(@name, offset+1)
      buf.write_byte(len, offset)
      offset += len + 1

      # Write particle type.
      buf.write_byte(@val_type, offset)
      offset+=1

      # Write filter begin.
      len = @begin.write(buf, offset+4)
      buf.write_int32(len, offset)
      offset += len + 4

      # Write filter end.
      len = @end.write(buf, offset+4)
      buf.write_int32(len, offset)
      offset += len + 4

      offset
    end

    # for internal use
    def collection_type
      case @col_type
      when :default then 0
      when :list then 1
      when :mapkeys then 2
      when :mapvalues then 3
      else 0
      end
    end

    #
    # Show the filter as String. This is util to show filters in logs.
    #
    def to_s
      return "#{@name} = #{@begin}" if @begin == @end
      "#{@name} = #{@begin} - #{@end}"
    end

    private

    def initialize(bin_name, begin_value, end_value, val_type = nil, col_type = nil)
      @name = bin_name
      @begin = Aerospike::Value.of(begin_value)
      @end = Aerospike::Value.of(end_value)

      # The type of the filter values can usually be inferred automatically;
      # but in certain cases caller can override the type.
      @val_type = val_type || @begin.type
      @col_type = col_type
    end

  end # class

end
