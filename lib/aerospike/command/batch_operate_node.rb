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

  class BatchOperateNode #:nodoc:

    attr_accessor :node, :records_by_idx

    def self.generate_list(cluster, replica_policy, records)
      records.each_with_index
             .group_by { |record, _| cluster.get_node_for_key(replica_policy, record.key, is_write: record.has_write) }
             .map { |node, records_with_idx| BatchOperateNode.new(node, records_with_idx) }
    end

    def initialize(node, records_with_idx)
      @node = node
      @records_by_idx = records_with_idx.to_h { |rec, idx| [idx, rec] }
    end

    def records
      records_by_idx.values
    end

    def each_record_with_index
      records_by_idx.each do |idx, rec|
        yield rec, idx
      end
    end

    def record_for_index(idx)
      @records_by_idx[idx]
    end

  end

end
