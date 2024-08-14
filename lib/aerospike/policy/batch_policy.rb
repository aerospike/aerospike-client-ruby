# Copyright 2014-2020 Aerospike, Inc.
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
    attr_accessor :allow_inline_ssd, :respond_all_keys, :send_key

    def initialize(opt={})
      super

      # [:nodoc:]
      # DEPRECATED
      # This setting does not have any effect anymore.
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
      @use_batch_direct = opt.fetch(:use_batch_direct, false)


      # Allow batch to be processed immediately in the server's receiving thread for SSD
      # namespaces. If false, the batch will always be processed in separate service threads.
      # Server versions &lt; 6.0 ignore this field.
      #
      # Inline processing can introduce the possibility of unfairness because the server
      # can process the entire batch before moving onto the next command.
      #
      # Default: false
      @allow_inline_ssd = opt.fetch(:allow_inline_ssd, false)


      # Should all batch keys be attempted regardless of errors. This field is used on both
      # the client and server. The client handles node specific errors and the server handles
      # key specific errors.
      #
      # If true, every batch key is attempted regardless of previous key specific errors.
      # Node specific errors such as timeouts stop keys to that node, but keys directed at
      # other nodes will continue to be processed.
      #
      # If false, the server will stop the batch to its node on most key specific errors.
      # The exceptions are {ResultCode#KEY_NOT_FOUND_ERROR} and
      # {ResultCode#FILTERED_OUT} which never stop the batch.
      # The client will stop the entire batch on node specific errors. The client will
      # not stop the entire batch commands run in parallel.
      #
      # Server versions < 6.0 do not support this field and treat this value as false
      # for key specific errors.
      #
      # Default: true
      @respond_all_keys = opt.fetch(:respond_all_keys, true)


      # Send user defined key in addition to hash digest on a record put.
      # The default is to _not_ send the user defined key.
      @send_key = opt.fetch(:send_key, false)

      self
    end

    def self.read_default
      BatchPolicy.new
    end

    def self.write_default
      bp = BatchPolicy.new
      bp.max_retries = 0
      bp
    end

  end # class

end # module
