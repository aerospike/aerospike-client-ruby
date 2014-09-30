# encoding: utf-8
# Copyright 2014 Aerospike, Inc.
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

require "spec_helper"

require 'aerospike/host'
require 'aerospike/key'
require 'aerospike/bin'
require 'aerospike/language'

describe Aerospike::Client do

  let(:client) do
    described_class.new("127.0.0.1", 3000)
  end

  let(:str_bin_name) do
    'str_bin'
  end

  let(:int_bin_name) do
    'int_bin'
  end

  before do
    (1..1000).to_a.each do |i|
      key = Support.gen_random_key
      client.put(key, {int_bin_name => rand(100000), str_bin_name => 'string value'})
    end
  end

  after do
    # clean up indexes
    key = Support.gen_random_key
    client.drop_index(key.namespace,
                      key.set_name,
                      "index_str_#{key.set_name}",
                      )
    client.drop_index(key.namespace,
                      key.set_name,
                      "index_int_#{key.set_name}",
                      )

    client.close
  end

  describe "Index operations" do

    it "should create an index and wait until it is created on all nodes" do
      key = Support.gen_random_key
      index_task = client.create_index(key.namespace,
                                       key.set_name,
                                       "index_int_#{key.set_name}",
                                       int_bin_name, :numeric
                                       )

      expect(index_task.wait_till_completed).to be true
      expect(index_task.completed?).to be true

      index_task = client.create_index(key.namespace,
                                       key.set_name,
                                       "index_str_#{key.set_name}",
                                       str_bin_name, :string
                                       )

      expect(index_task.wait_till_completed).to be true
      expect(index_task.completed?).to be true
    end

  end # describe

end # describe
