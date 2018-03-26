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
      module Peers
        class << self
          def call(node, peers)
            return unless should_refresh?(node)

            ::Aerospike.logger.debug("Update peers for node #{node.name}")

            cluster = node.cluster

            collection = ::Aerospike::Peers::Fetch.(cluster, node.tend_connection)
            peers.peers = collection.peers
            node.peers_count.value = peers.peers.size
            peers_validated = true

            peers.peers.each do |peer|
              next if ::Aerospike::Cluster::FindNode.(cluster, peers, peer.node_name)

              node_validated = false

              peer.hosts.each do |host|
                begin
                  nv = NodeValidator.new(cluster, host, cluster.connection_timeout, cluster.cluster_name, cluster.ssl_options)

                  if nv.name != peer.node_name
                    ::Aerospike.logger.warn("Peer node #{peer.node_name} is different than actual node #{nv.name} for host #{host}");
                    # Must look for new node name in the unlikely event that node names do not agree.
                    # Node already exists. Do not even try to connect to hosts.
                    if Cluster::FindNode.(cluster, peers, nv.name)
                      node_validated = true
                      break;
                    end
                  end

                  new_node = cluster.create_node(nv)
                  peers.nodes[nv.name] = new_node
                  node_validated = true
                  break;
                rescue ::Aerospike::Exceptions::Aerospike => e
                  Aerospike.logger.warn("Add node #{host} failed: #{e.inspect}")
                end

                peers_validated = false unless node_validated
              end
            end

            # Only set new peers generation if all referenced peers are added to
            # the cluster.
            node.peers_generation.update(collection.generation) if peers_validated
            peers.refresh_count += 1
          rescue ::Aerospike::Exceptions::Aerospike => e
            Refresh::Failed.(node, e)
          end

          def should_refresh?(node)
            node.failures.value == 0 && node.active?
          end
        end
      end
    end
  end
end
