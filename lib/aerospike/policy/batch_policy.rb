# Copyright 2014-2018 Aerospike, Inc.
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

require 'aerospike/policy/policy'

module Aerospike

  # Container object for batch policy command.
  class BatchPolicy < Policy

    attr_accessor :use_batch_direct, :record_queue_size

    def initialize(opt={})
      super(opt)

      # Use old batch direct protocol where batch reads are handled by direct
      # low-level batch server database routines. The batch direct protocol can
      # be faster when there is a single namespace. But there is one important
      # drawback: The batch direct protocol will not proxy to a different
      # server node when the mapped node has migrated a record to another node
      # (resulting in not found record). This can happen after a node has been
      # added/removed from the cluster and there is a lag between records being
      # migrated and client partition map update (once per second). The batch
      # index protocol will perform this record proxy when necessary.
      #
      # Default: false (use new batch index protocol if server supports it)
      @use_batch_direct = opt.fetch(:use_batch_direct) { false }

      self
    end

  end # class

end # module
