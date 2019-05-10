# encoding: utf-8
# Copyright 2015 Aerospike, Inc.
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

require "aerospike/query/statement"

describe Aerospike::GeoJSON do

  let(:point) { described_class.new(type: 'Point', coordinates: [103.9114, 1.3083]) }
  let(:circle) { described_class.new(type: 'AeroCircle', coordinates: [[103.9114, 1.3083], 100]) }
  let(:polygon) do
    described_class.new(
      type: 'Polygon',
      coordinates: [
        [
          [103.9114, 1.3083],
          [103.9214, 1.3083],
          [103.9214, 1.3183],
          [103.9114, 1.3083]
        ]
      ]
    )
  end

  describe "#initialize" do

    it "should accept GeoJSON data as Hash" do
      data = {type: "Point", coordinates: [103.9114, 1.3083]}

      geo_json = described_class.new(data)

      expect(geo_json.to_json).to eq %q({"type":"Point","coordinates":[103.9114,1.3083]})
    end # it

    it "should accept GeoJSON data as String" do
      data = %q({"type":"Point","coordinates":[103.9114,1.3083]})

      geo_json = described_class.new(data)

      expect(geo_json.to_json).to eq %q({"type":"Point","coordinates":[103.9114,1.3083]})
    end # it

  end # describe

  describe '#lng' do
    it 'should return longitude of point' do
      expect(point.lng).to eq(103.9114)
    end

    it 'should return longitude of circle' do
      expect(circle.lng).to eq(103.9114)
    end

    it 'should return nil if polygon' do
      expect(polygon.lng).to eq(nil)
    end
  end

  describe '#lat' do
    it 'should return longitude of point' do
      expect(point.lat).to eq(1.3083)
    end

    it 'should return longitude of circle' do
      expect(circle.lat).to eq(1.3083)
    end

    it 'should return nil if polygon' do
      expect(polygon.lat).to eq(nil)
    end
  end

  describe '#range' do
    it 'should return nil if point' do
      expect(point.range).to eq(nil)
    end

    it 'should return longitude of circle' do
      expect(circle.range).to eq(100)
    end

    it 'should return nil if polygon' do
      expect(polygon.range).to eq(nil)
    end
  end

end # describe
