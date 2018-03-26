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
  class Cluster
    # Find node in cluster by name
    module FindNode
      class << self
        def call(cluster, peers, node_name)
          node = cluster.find_node_by_name(node_name) || peers.find_node_by_name(node_name)
          return if node.nil?
          node.tap do |n|
            n.increase_reference_count!
          end
        end
      end
    end
  end
end
