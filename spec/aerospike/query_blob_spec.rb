# encoding: utf-8
# Copyright 2014-2023 Aerospike, Inc.
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

describe 'TestQueryBlob' do
  before(:all) do
    @index_name = 'qbindex'
    @bin_name = 'bb'
    @index_name_list = 'qblist'
    @bin_name_list = 'bblist'
    @size = 5
    @namespace = 'test'
    @set = 'query-blob'

    Support.client.drop_index(@namespace, @set, @index_name)
    Support.client.drop_index(@namespace, @set, @index_name_list)
    Support.client.create_index(@namespace, @set, @index_name, @bin_name, :blob)
    Support.client.create_index(@namespace, @set, @index_name_list, @bin_name_list, :blob, :list)

    (1..@size).each do |i|
      bytes = bytes_to_str([0b00000001, 0b01000010])
      blob = Aerospike::BytesValue.new(bytes)
      blob_list = [blob]

      key = Aerospike::Key.new(@namespace, @set, i)
      bin = Aerospike::Bin.new(@bin_name, blob)
      bin_list = Aerospike::Bin.new(@bin_name_list, blob_list)
      Support.client.put(key, [bin, bin_list])
    end
  end

  def bytes_to_str(bytes)
    bytes.pack("C*").force_encoding("binary")
  end

  it 'should query blob' do
    bytes = bytes_to_str([0b00000001, 0b01000010])
    blob = Aerospike::BytesValue.new(bytes)

    stmt = Aerospike::Statement.new('test', 'query-blob', ['bb'])
    stmt.filters << Aerospike::Filter.Equal('bb', blob)
    rs = Support.client.query(stmt)

    begin
      count = 0

      rs.each do |record|
        result = Aerospike::BytesValue.new(record.bins['bb'])
        expect(result.to_bytes).to eq(blob.to_bytes)
        count += 1
      end
      expect(count).not_to eq(0)
    end
  end

  it 'should query blob in list' do
    bytes = bytes_to_str([0b00000001, 0b01000010])
    blob = Aerospike::BytesValue.new(bytes)

    stmt = Aerospike::Statement.new('test', 'query-blob', ['bblist'])
    stmt.filters << Aerospike::Filter.Contains('bblist', blob, :list)
    rs = Support.client.query(stmt)

    begin
      count = 0

      rs.each do |record|
        result_list = record.bins['bblist']
        expect(result_list.size).to eq(1)
        count += 1
      end
      expect(count).not_to eq(0)
    end
  end
end