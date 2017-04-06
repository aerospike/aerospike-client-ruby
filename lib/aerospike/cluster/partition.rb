# encoding: utf-8
# Copyright 2014-2017 Aerospike, Inc.
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

  private

  class Partition # :nodoc:
    attr_reader :namespace, :partition_id

    def initialize(namespace, partition_id)
      @namespace = namespace
      @partition_id = partition_id

      self
    end

    def self.new_by_key(key)
      Partition.new(
        key.namespace,
        (key.digest[0..3].unpack('l<')[0] & 0xFFFF) % Node::PARTITIONS
      )
    end

    def to_s
      "#{@namespace}:#{partition_id}"
    end

    def ==(other)
      other && other.is_a?(Partition) && @partition_id == other.partition_id &&
        @namespace == other.namespace
    end
    alias eql? ==

    def hash
      to_s.hash
    end

  end # class

end # module
