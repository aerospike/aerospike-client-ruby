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

require 'aerospike/record'

require 'aerospike/command/command'

module Aerospike

  private


  class BatchIndexNode #:nodoc:

    attr_accessor :node, :keys, :offsets

    def self.generate_list(cluster, keys)
      nodes = cluster.nodes

      if nodes.length == 0
        raise Aerospike::Exceptions::Connection.new("command failed because cluster is empty.")
      end

      # Split keys by server node.
      batch_nodes = []

      keys.each_with_index do |key, i|

        partition = Partition.new_by_key(key)

        # error not required
        node = cluster.get_node(partition)
        batch_node = batch_nodes.detect{|bn| bn.node == node}

        unless batch_node
          batch_nodes << BatchIndexNode.new(node, key, i)
        else
          batch_node.add_key(key,i)
        end
      end
      batch_nodes
    end

    def each_key_with_offset()
      i = 0

      @offset_key_hash.each do |key,value|
        yield value,key
      end
    end

    def initialize(node, key, offset)
      @node = node
      @keys = [key]
      @offset_key_hash = {}
      @offset_key_hash[offset] = key
    end

    def add_key(key,offset)
      @keys << key
      @offset_key_hash[offset] = key
    end

    def get_key_from_offset(offset)
      return @offset_key_hash[offset]
    end
    
  end # class

end # module
