# encoding: utf-8
# Copyright 2014-2020 Aerospike, Inc.
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
    attr_reader :packed_ctx

    # open up the class to alias the class methods for naming consistency
    class << self
      def equal(bin_name, value, ctx: nil)
        Filter.new(bin_name, value, value, nil, nil, ctx)
      end

      def contains(bin_name, value, col_type, ctx: nil)
        Filter.new(bin_name, value, value, nil, col_type, ctx)
      end

      def range(bin_name, from, to, col_type = nil, ctx: nil)
        Filter.new(bin_name, from, to, nil, col_type, ctx)
      end

      def geo_within_geo_region(bin_name, region, col_type = nil, ctx: nil)
        region = region.to_json
        Filter.new(bin_name, region, region, ParticleType::GEOJSON, col_type, ctx)
      end

      def geo_within_radius(bin_name, lon, lat, radius_meter, col_type = nil, ctx: nil)
        region = GeoJSON.new({ type: "AeroCircle", coordinates: [[lon, lat], radius_meter] })
        geo_within_geo_region(bin_name, region, col_type, ctx: ctx)
      end

      def geo_contains_geo_point(bin_name, point, col_type = nil, ctx: nil)
        point = point.to_json
        Filter.new(bin_name, point, point, ParticleType::GEOJSON, col_type, ctx)
      end

      def geo_contains_point(bin_name, lon, lat, col_type = nil, ctx: nil)
        point = GeoJSON.new({ type: "Point", coordinates: [lon, lat] })
        geo_contains_geo_point(bin_name, point, col_type, ctx: ctx)
      end

      # alias the old names for compatibility
      alias :Equal :equal
      alias :Contains :contains
      alias :Range :range
      alias :geoWithinGeoJSONRegion :geo_within_geo_region
      alias :geoWithinRadius :geo_within_radius
      alias :geoContainsGeoJSONPoint :geo_contains_geo_point
      alias :geoContainsPoint :geo_contains_point
    end

    def estimate_size
      return @name.bytesize + @begin.estimate_size + @end.estimate_size + 10
    end

    def write(buf, offset)
      # Write name.
      len = buf.write_binary(@name, offset + 1)
      buf.write_byte(len, offset)
      offset += len + 1

      # Write particle type.
      buf.write_byte(@val_type, offset)
      offset += 1

      # Write filter begin.
      len = @begin.write(buf, offset + 4)
      buf.write_int32(len, offset)
      offset += len + 4

      # Write filter end.
      len = @end.write(buf, offset + 4)
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

    def initialize(bin_name, begin_value, end_value, val_type = nil, col_type = nil, ctx = nil)
      @name = bin_name
      @begin = Aerospike::Value.of(begin_value)
      @end = Aerospike::Value.of(end_value)

      # The type of the filter values can usually be inferred automatically;
      # but in certain cases caller can override the type.
      @val_type = val_type || @begin.type
      @col_type = col_type

      @packed_ctx = CDT::Context.bytes(ctx)
    end
  end # class
end
