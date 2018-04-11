# Copyright 2014-2018 Aerospike, Inc.
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

  BatchNamespace = Struct.new :namespace, :keys

  class BatchDirectNode #:nodoc:

    attr_accessor :node
    attr_accessor :batch_namespaces

    def self.generate_list(cluster, keys)
      keys.group_by { |key| cluster.get_node_for_key(key) }
        .map { |node, keys_for_node| BatchDirectNode.new(node, keys_for_node) }
    end

    def initialize(node, keys)
      @node = node
      @batch_namespaces = keys.group_by(&:namespace)
        .map { |ns, keys_for_ns| BatchNamespace.new(ns, keys_for_ns) }
    end

  end

end
