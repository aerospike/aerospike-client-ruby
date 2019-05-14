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
  let(:lng) { 103.9114 }
  let(:lat) { 1.3083 }
  let(:radius) { 100 }
  let(:coordinates) do
    [
      [
        [lng, lat],
        [103.9214, lat],
        [103.9214, 1.3183],
        [lng, lat]
      ]
    ]
  end

  let(:point) { described_class.new(type: 'Point', coordinates: [lng, lat]) }
  let(:circle) { described_class.new(type: 'AeroCircle', coordinates: [[lng, lat], radius]) }
  let(:polygon) { described_class.new(type: 'Polygon',coordinates: coordinates) }

  describe "#initialize" do

    it "should accept GeoJSON data as Hash" do
      data = {type: "Point", coordinates: [lng, lat]}

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
      expect(point.lng).to eq(lng)
    end

    it 'should return longitude of circle' do
      expect(circle.lng).to eq(lng)
    end

    it 'should return nil if polygon' do
      expect(polygon.lng).to eq(nil)
    end
  end

  describe '#lat' do
    it 'should return longitude of point' do
      expect(point.lat).to eq(lat)
    end

    it 'should return longitude of circle' do
      expect(circle.lat).to eq(lat)
    end

    it 'should return nil if polygon' do
      expect(polygon.lat).to eq(nil)
    end
  end

  describe '#radius' do
    it 'should return nil if point' do
      expect(point.radius).to eq(nil)
    end

    it 'should return radius of circle' do
      expect(circle.radius).to eq(radius)
    end

    it 'should return nil if polygon' do
      expect(polygon.radius).to eq(nil)
    end
  end

  describe '.point' do
    let(:new_point) { described_class.point(lng, lat) }

    it 'returns a new GeoJSON object' do
      expect(new_point).to be_a(described_class)
    end

    it 'new GeoJSON object is a Point' do
      expect(new_point.point?).to eq(true)
    end

    it 'has correct longitude' do
      expect(new_point.lng).to eq(lng)
    end

    it 'has correct latitude' do
      expect(new_point.lat).to eq(lat)
    end
  end

  describe '.circle' do
    let(:new_circle) { described_class.circle(lng, lat, radius) }

    it 'returns a new GeoJSON object' do
      expect(new_circle).to be_a(described_class)
    end

    it 'new GeoJSON object is a Circle' do
      expect(new_circle.circle?).to eq(true)
    end

    it 'has correct longitude' do
      expect(new_circle.lng).to eq(lng)
    end

    it 'has correct latitude' do
      expect(new_circle.lat).to eq(lat)
    end

    it 'has correct radius' do
      expect(new_circle.radius).to eq(radius)
    end
  end

  describe '.polygon' do
    let(:new_polygon) { described_class.polygon(coordinates) }

    it 'returns a new GeoJSON object' do
      expect(new_polygon).to be_a(described_class)
    end

    it 'new GeoJSON object is a Polygon' do
      expect(new_polygon.polygon?).to eq(true)
    end

    it 'has correct coordinates' do
      expect(new_polygon.coordinates).to eq(coordinates)
    end
  end

  describe '#to_circle' do
    let(:new_circle) { point.to_circle(radius) }

    it 'returns a GeoJSON object' do
      expect(new_circle).to be_a(described_class)
    end

    it 'new GeoJSON object is a Circle' do
      expect(new_circle.circle?).to eq(true)
    end

    it 'has correct longitude' do
      expect(new_circle.lng).to eq(lng)
    end

    it 'has correct latitude' do
      expect(new_circle.lat).to eq(lat)
    end

    it 'has correct radius' do
      expect(new_circle.radius).to eq(radius)
    end

    context 'when called from another Circle' do
      let(:new_radius) { 1234 }
      let(:new_circle) { circle.to_circle(new_radius) }

      it 'returns a GeoJSON object' do
        expect(new_circle).to be_a(described_class)
      end

      it 'new GeoJSON object is a Circle' do
        expect(new_circle.circle?).to eq(true)
      end

      it 'has correct longitude' do
        expect(new_circle.lng).to eq(lng)
      end

      it 'has correct latitude' do
        expect(new_circle.lat).to eq(lat)
      end

      it 'has correct radius' do
        expect(new_circle.radius).to eq(new_radius)
      end
    end

    context 'when called from a Polygon' do
      it 'returns an error' do
        expect {
          polygon.to_circle(radius)
        }.to raise_error(TypeError)
      end
    end
  end

  describe '#to_point' do
    let(:new_point) { circle.to_point }

    it 'returns a GeoJSON object' do
      expect(new_point).to be_a(described_class)
    end

    it 'new GeoJSON object is a Point' do
      expect(new_point.point?).to eq(true)
    end

    it 'has correct longitude' do
      expect(new_point.lng).to eq(lng)
    end

    it 'has correct latitude' do
      expect(new_point.lat).to eq(lat)
    end

    context 'when called from another Point' do
      let(:new_point) { point.to_point }

      it 'returns a GeoJSON object' do
        expect(new_point).to be_a(described_class)
      end

      it 'new GeoJSON object is a Point' do
        expect(new_point.point?).to eq(true)
      end

      it 'has correct longitude' do
        expect(new_point.lng).to eq(lng)
      end

      it 'has correct latitude' do
        expect(new_point.lat).to eq(lat)
      end
    end

    context 'when called from a Polygon' do
      it 'returns an error' do
        expect {
          polygon.to_circle(radius)
        }.to raise_error(TypeError)
      end
    end
  end
end # describe
