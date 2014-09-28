# encoding: utf-8
# Copyright 2014 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require 'thread'

require 'apik/record'

require 'apik/command/command'

module Apik

  protected

  BatchNamespace = Struct.new :namespace, :keys

  class BatchNode

    attr_accessor :node, :batch_namespaces, :key_capacity

    def self.generate_list(cluster, keys)
      nodes = cluster.nodes

      if nodes.length == 0
        raise Apik::Exceptions::Connection.new("command failed because cluster is empty.")
      end

      node_count = nodes.length
      keys_per_node = (keys.length/node_count).to_i + 10

      # Split keys by server node.
      batch_nodes = []

      keys.each do |key|
        partition = Partition.new_by_key(key)

        # error not required
        node = cluster.get_node(partition)
        batch_node = batch_nodes.detect{|bn| bn.node == node}

        unless batch_node
          batch_nodes << BatchNode.new(node, keys_per_node, key)
        else
          batch_node.add_key(key)
        end
      end

      batch_nodes
    end


    def initialize(node, key_capacity, key)
      @node = node
      @key_capacity = key_capacity
      @batch_namespaces = [BatchNamespace.new(key.namespace, [key])]
    end

    def add_key(key)
      batch_namespace = @batch_namespaces.detect{|bn| bn.namespace == key.namespace }

      unless batch_namespace
        @batch_namespaces << BatchNamespace.new(key.namespace, [key])
      else
        batch_namespace.keys << key
      end
    end

  end # class

end # module
