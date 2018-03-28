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
    # Calculates which nodes that should be removed from the cluster
    module FindNodesToRemove
      class << self
        def call(cluster, refresh_count)
          node_list = cluster.nodes

          remove_list = []

          node_list.each do |node|
            unless node.active?
              # Inactive nodes must be removed.
              remove_list << node
              next
            end

            if refresh_count.zero? && node.failed?(4) # 5 or more failures counts as failed
              # All node info requests failed and this node had 5 consecutive failures.
              # Remove node.  If no nodes are left, seeds will be tried in next cluster
              # tend iteration.
              remove_list << node
              next
            end

            if node_list.size > 1 && refresh_count >= 1 && !node.referenced?
              # Node is not referenced by other nodes.
              # Check if node responded to info request.
              if node.failed?
                remove_list << node
              else
                # Node is alive, but not referenced by other nodes.  Check if mapped.
                unless cluster.find_node_in_partition_map(node)
                  # Node doesn't have any partitions mapped to it.
                  # There is no point in keeping it in the cluster.
                  remove_list << node
                end
              end
            end
          end

          remove_list
        end
      end
    end
  end
end
