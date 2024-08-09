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

require "aerospike/policy/priority"
require "aerospike/policy/consistency_level"
require "aerospike/policy/replica"

module Aerospike

  # Container object for client policy command.
  class Policy
    attr_accessor :filter_exp, :priority, :timeout, :max_retries, :sleep_between_retries, :consistency_level,
                  :fail_on_filtered_out, :replica, :use_compression, :socket_timeout

    alias total_timeout timeout
    alias total_timeout= timeout=

    def initialize(opt = {})
      # Container object for transaction policy attributes used in all database
      # operation calls.

      # Optional expression filter. If filter_exp exists and evaluates to false, the
      # transaction is ignored.
      #
      # Default: nil
      #
      # ==== Examples:
      #
      # p = Policy.new
      # p.filter_exp = Exp.build(Exp.eq(Exp.int_bin("a"), Exp.int_val(11)));
      @filter_exp = opt[:filter_exp]

      #  Throw exception if {#filter_exp} is defined and that filter evaluates
      #  to false (transaction ignored).  The {AerospikeException}
      #  will contain result code {ResultCode::FILTERED_OUT}.
      #
      #  This field is not applicable to batch, scan or query commands.
      #
      #  Default: false
      @fail_on_filtered_out = opt[:fail_on_filtered_out] || false

      # [:nodoc:]
      # DEPRECATED
      # The Aerospike server does not support this policy anymore
      # TODO: Remove for next major release
      @priority = opt[:priority] || Priority::DEFAULT

      # Throw exception if @filter_exp is defined and that filter evaluates
      # to false (transaction ignored). The Aerospike::Exceptions::Aerospike
      # will contain result code Aerospike::ResultCode::FILTERED_OUT.
      # This field is not applicable to batch, scan or query commands.
      @fail_on_filtered_out = opt[:fail_on_filtered_out] || false

      # How replicas should be consulted in a read operation to provide the desired
      # consistency guarantee. Default to allowing one replica to be used in the
      # read operation.
      @consistency_level = opt[:consistency_level] || Aerospike::ConsistencyLevel::CONSISTENCY_ONE

      # Send read commands to the node containing the key's partition replica type.
      # Write commands are not affected by this setting, because all writes are directed
      # to the node containing the key's master partition.
      #
      # Default to sending read commands to the node containing the key's master partition.
      @replica = opt[:replica] || Aerospike::Replica::MASTER

      # Use zlib compression on write or batch read commands when the command buffer size is greater
      # than 128 bytes. In addition, tell the server to compress its response on read commands.
      # The server response compression threshold is also 128 bytes.
      #
      # This option will increase cpu and memory usage (for extra compressed buffers), but
      # decrease the size of data sent over the network.
      @use_compression = opt[:use_compression] || false

      # Transaction timeout.
      # This timeout is used to set the socket timeout and is also sent to the
      # server along with the transaction in the wire protocol.
      # Default to no timeout (0).
      @timeout = opt[:timeout] || 0

      # Maximum number of retries before aborting the current transaction.
      # A retry is attempted when there is a network error other than timeout.
      # If max_retries is exceeded, the abort will occur even if the timeout
      # has not yet been exceeded.
      @max_retries = opt[:max_retries] || 2

      # Duration to sleep between retries if a transaction fails and the
      # timeout was not exceeded. Enter zero to skip sleep.
      @sleep_between_retries = opt[:sleep_between_retries] || 0.5

      # Determines network timeout for each attempt.
      #
      # If socket_timeout is not zero and socket_timeout is reached before an attempt completes,
      # the Timeout above is checked. If Timeout is not exceeded, the transaction
      # is retried. If both socket_timeout and Timeout are non-zero, socket_timeout must be less
      # than or equal to Timeout, otherwise Timeout will also be used for socket_timeout.
      #
      # Default: 30s
      @socket_timeout = opt[:socket_timeout] || 30000
    end
  end # class
end # module
