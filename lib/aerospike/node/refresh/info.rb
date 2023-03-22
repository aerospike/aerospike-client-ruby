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
  class Node
    module Refresh
      module Info
        CMDS_BASE = %w[node partition-generation cluster-name].freeze
        CMDS_PEERS = (CMDS_BASE + ['peers-generation']).freeze
        CMDS_SERVICES = (CMDS_BASE + ['services']).freeze
        CMDS_REBALANCE = (CMDS_PEERS + ['rebalance-generation']).freeze

        class << self
          def call(node, peers)
            conn = node.tend_connection
            if peers.use_peers?
              cmds = node.cluster.rack_aware ? CMDS_REBALANCE : CMDS_PEERS
              info_map = ::Aerospike::Info.request(conn, *cmds)
              Verify::PeersGeneration.(node, info_map, peers)
              Verify::PartitionGeneration.(node, info_map)
              Verify::RebalanceGeneration.(node, info_map) if node.cluster.rack_aware
              Verify::Name.(node, info_map)
              Verify::ClusterName.(node, info_map)
            else
              info_map = ::Aerospike::Info.request(conn, *CMDS_SERVICES)
              Verify::PartitionGeneration.(node, info_map)
              Verify::Name.(node, info_map)
              Verify::ClusterName.(node, info_map)
              Refresh::Friends.(node, peers, info_map)
            end

            node.restore_health
            node.responded!

            peers.refresh_count += 1
            node.reset_failures!
          rescue ::Aerospike::Exceptions::Aerospike => e
            node.close_connection(conn) if conn
            node.decrease_health
            peers.generation_changed = true if peers.use_peers?
            Refresh::Failed.(node, e)
          end
        end
      end
    end
  end
end
