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

  class BatchIndexNode #:nodoc:

    attr_accessor :node
    attr_accessor :keys_by_idx

    def self.generate_list(cluster, keys)
      keys.each_with_index
        .group_by { |key, _| cluster.get_node_for_key(key) }
        .map { |node, keys_with_idx| BatchIndexNode.new(node, keys_with_idx) }
    end

    def initialize(node, keys_with_idx)
      @node = node
      @keys_by_idx = Hash[keys_with_idx.map(&:reverse)]
    end

    def keys
      keys_by_idx.values
    end

    def each_key_with_index
      keys_by_idx.each do |idx, key|
        yield key, idx
      end
    end

    def key_for_index(idx)
      keys_by_idx[idx]
    end

  end

end
