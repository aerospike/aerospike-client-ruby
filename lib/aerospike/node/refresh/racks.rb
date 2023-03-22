# frozen_string_literal: true

# Copyright 2018-2020 Aerospike, Inc.
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
      module Racks
        class << self
          def call(node)
            return unless should_refresh?(node)

            Aerospike.logger.info("Updating racks for node #{node.name}")
            conn = node.tend_connection
            parser = RackParser.new(node, conn)
            node.update_racks(parser)
          rescue ::Aerospike::Exceptions::Aerospike => e
            node.close_connection(conn)
            Refresh::Failed.(node, e)
          end

          # Do not refresh racks when node connection has already failed
          # during this cluster tend iteration.
          def should_refresh?(node)
            return false if node.failed? || !node.active?
            true
          end
        end
      end
    end
  end
end
