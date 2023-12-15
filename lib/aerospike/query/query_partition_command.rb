# encoding: utf-8
# Copyright 2014-2020 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License") you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require "aerospike/query/stream_command"
require "aerospike/query/recordset"

module Aerospike
  private

  class QueryPartitionCommand < QueryCommand #:nodoc:
    def initialize(cluster, node, tracker, policy, statement, recordset, node_partitions)
      super(cluster, node, policy, statement, recordset, @node_partitions)
      @node_partitions = node_partitions
      @tracker = tracker
    end

    def write_buffer
      set_query(@cluster, @policy, @statement, false, @node_partitions)
    end



    def should_retry(e)
      # !! converts nil to false
      !!@tracker&.should_retry(@node_partitions, e)
    end
  end # class
end # module
