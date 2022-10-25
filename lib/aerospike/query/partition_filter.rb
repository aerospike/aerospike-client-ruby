# frozen_string_literal: true

# Copyright 2014-2020 Aerospike, Inc.
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

module Aerospike
  class PartitionFilter
    attr_reader :partition_begin, :count, :digest
    attr_accessor :partitions, :done

    alias done? done

    # Creates a partition filter that
    # reads all the partitions.
    def self.all
      PartitionFilter.new(0, Aerospike::Node::PARTITIONS)
    end

    # Creates a partition filter by partition id.
    # Partition id is between 0 - 4095
    def self.by_id(partition_id)
      PartitionFilter.new(partition_id, 1)
    end

    # Creates a partition filter by partition range.
    # begin partition id is between 0 - 4095
    # count is the number of partitions, in the range of 1 - 4096 inclusive.
    def self.by_range(partition_begin, count)
      PartitionFilter.new(partition_begin, count)
    end

    # Creates a partition filter that will return
    # records after key's digest in the partition containing the digest.
    # Note that digest order is not the same as userKey order. This method
    # only works for scan or query with nil filter.
    def self.by_key(key)
      PartitionFilter.new(key.partition_id, 1, key.digest)
    end

    def to_s
      "PartitionFilter<begin: #{@partition_begin}, count: #{@count}, digest: #{@digest}, done: #{@done}>"
    end

    private

    def initialize(partition_begin, count, digest = nil, partitions = nil, done = false)
      @partition_begin = partition_begin
      @count = count
      @digest = digest
      @partitions = partitions
      @done = done
    end
  end
end
