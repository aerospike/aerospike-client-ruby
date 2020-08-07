# encoding: utf-8
# Copyright 2014-2020 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Aerospike

  module Replica

    # Read from node containing key's master partition.  This is the default behavior.
    MASTER = 0

    # Distribute reads across nodes containing key's master and replicated partitions
    # in round-robin fashion.
    MASTER_PROLES = 1

    # Always try node containing master partition first. If connection fails and
    # Policy#retryOnTimeout is true, try nodes containing prole partition.
    SEQUENCE = 2

    # Try node on the same rack as the client first.  If there are no nodes on the
    # same rack, use SEQUENCE instead.
    #
    # ClientPolicy#rack_aware, ClientPolicy#rack_id, and server rack
    # configuration must also be set to enable this functionality.
    PREFER_RACK = 3

    # Distribute reads across all nodes in cluster in round-robin fashion.
    # This option is useful when the replication factor equals the number
    # of nodes in the cluster and the overhead of requesting proles is not desired.
    RANDOM = 4

  end # module

end # module
