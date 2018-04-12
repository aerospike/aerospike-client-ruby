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
      # Refresh peers/friends based on old service protocol
      module Friends
        class << self
          def call(node, peers, info_map)
            friend_string = info_map['services']
            cluster = node.cluster

            Aerospike.logger.debug("Refreshing friends for node #{node.name}: services=#{friend_string}")

            if friend_string.to_s.empty?
              node.peers_count.value = 0
              return
            end

            friend_names = friend_string.split(';')
            node.peers_count.value = friend_names.size

            friend_names.each do |friend|
              hostname, port = friend.split(':')
              host = Host.new(hostname, port.to_i)
              found_node = cluster.find_alias(host)

              if found_node
                found_node.increase_reference_count!
                Aerospike.logger.debug("Found existing node #{found_node.name} for host #{host}: Increased ref count to #{found_node.reference_count.value}")
              else
                unless peers.hosts.include?(host)
                  prepare(cluster, peers, host)
                end
              end
            end
          end

          def prepare(cluster, peers, host)
            Aerospike.logger.debug("Preparing to add new node for host #{host}")
            nv = NodeValidator.new(
              cluster,
              host,
              cluster.connection_timeout,
              cluster.cluster_name,
              cluster.tls_options
            )

            node = peers.find_node_by_name(nv.name)

            unless node.nil?
              Aerospike.logger.debug("Found existing node #{node.name} among peers for host #{host}")
              peers.hosts << host
              node.aliases << host
              return true
            end

            node = cluster.find_node_by_name(nv.name)

            unless node.nil?
              Aerospike.logger.debug("Found existing node #{node.name} in cluster for host #{host}")
              peers.hosts << host
              node.aliases << host
              # Only increase reference count if found in cluster
              node.increase_reference_count!
              cluster.add_alias(host, node)
              return true
            end

            Aerospike.logger.debug("No existing node found - creating new node #{nv.name} for host #{host}")
            node = cluster.create_node(nv)
            peers.hosts << host
            peers.nodes[nv.name] = node
            true
          rescue ::Aerospike::Exceptions::Aerospike => e
            ::Aerospike.logger.warn("Add node for host #{host} failed: #{e}")
            false
          end
        end
      end
    end
  end
end
