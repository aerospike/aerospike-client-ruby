# Copyright 2012-2014 Aerospike, Inc.
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

module Apik

  class Partition
    attr_reader :namespace, :partition_id

    def initialize(namespace, partitionId)
      @namespace = namespace
      @partition_id = partition_id
    end

    def self.new_by_key(key)
      Partition.new(key.namespace, key.digest_to_intel_int)
    end

    def to_s
      "#{@namespace}:#{partition_id}"
    end

    def ==(other)
      other && other.is_a?(Partition) && @partition_id == other.partition_id &&
        @namespace == other.namespace
    end

  end # class

end # module
