# frozen_string_literal: true

# Copyright 2018 Aerospike, Inc.
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

module Aerospike
  class Peers
    attr_accessor :peers, :hosts, :nodes, :refresh_count, :use_peers, :generation_changed

    def initialize
      @peers = ::Array.new
      @hosts = ::Set.new
      @nodes = {}
      @use_peers = true
      @refresh_count = 0
    end

    def find_node_by_name(node_name)
      @nodes[node_name]
    end

    def generation_changed?
      @generation_changed == true
    end

    def use_peers?
      @use_peers == true
    end
  end
end
