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
            if !node.active?
              # Inactive nodes must be removed.
              remove_list << node
              next
            end

            case node_list.length
            when 1
              # Single node clusters rely solely on node health.
              remove_list << node if node.unhealthy?

            when 2
              # Two node clusters require at least one successful refresh before removing.
              if refresh_count == 2 && node.reference_count.value == 0 && !node.responded?
                # Node is not referenced nor did it respond.
                remove_list << node
              end

            else
              # Multi-node clusters require two successful node refreshes before removing.
              if refresh_count >= 2 && node.reference_count.value == 0
                # Node is not referenced by other nodes.
                # Check if node responded to info request.
                if node.responded?
                  # Node is alive, but not referenced by other nodes.  Check if mapped.
                  unless cluster.find_node_in_partition_map(node)
                    # Node doesn't have any partitions mapped to it.
                    # There is not point in keeping it in the cluster.
                    remove_list << node
                  end
                else
                  # Node not responding. Remove it.
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
