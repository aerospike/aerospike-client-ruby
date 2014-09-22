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

require 'apik/cluster/partition'
require 'apik/command/command'

module Apik

  protected

  class SingleCommand < Command

    def initialize(cluster, key)
      @cluster = cluster
      @key = key
      @partition = Partition.new_by_key(key)
      @node = @cluster.get_node(@partition)

      self
    end


    protected

    def emptySocket
      # There should not be any more bytes.
      # Empty the socket to be safe.
      sz = @dataBuffer.read_int64( 0)
      headerLength = @dataBuffer.read(8).ord
      receiveSize = Integer(sz&0xFFFFFFFFFFFF) - headerLength

      # Read remaining message bytes.
      if receiveSize > 0
        sizeBufferSz(receiveSize)
        @conn.read(@dataBuffer, receiveSize)
      end
    end

  end # class

end # module
