# frozen_string_literal: true

# Copyright 2018-2019 Aerospike, Inc.
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
      module Partitions
        class << self
          def call(node, peers)
            return unless should_refresh?(node, peers)

            Aerospike.logger.info("Updating partitions for node #{node.name}")
            conn = node.tend_connection
            parser = PartitionParser.new(node, conn)
            node.cluster.update_partitions(parser)
          rescue ::Aerospike::Exceptions::Aerospike => e
            conn.close
            Refresh::Failed.(node, e)
          end

          # Do not refresh partitions when node connection has already failed
          # during this cluster tend iteration. Also, avoid "split cluster"
          # case where this node thinks it's a 1-node cluster. Unchecked, such
          # a node can dominate the partition map and cause all other nodes to
          # be dropped.
          def should_refresh?(node, peers)
            return false if node.failed? || !node.active?
            return false if !node.has_peers? && peers.refresh_count > 1
            true
          end
        end
      end
    end
  end
end
