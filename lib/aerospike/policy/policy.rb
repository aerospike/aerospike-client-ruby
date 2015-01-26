# encoding: utf-8
# Copyright 2014 Aerospike, Inc.
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

require 'aerospike/policy/priority'
require 'aerospike/policy/consistency_level'


module Aerospike

  # Container object for client policy command.
  class Policy

    attr_accessor :priority, :timeout, :max_retries, :sleep_between_retries, :consistency_level

    def initialize(opt={})
      # Container object for transaction policy attributes used in all database
      # operation calls.

      # Priority of request relative to other transactions.
      # Currently, only used for scans.
      @priority = opt[:priority] || Priority::DEFAULT

      # How replicas should be consulted in a read operation to provide the desired
      # consistency guarantee. Default to allowing one replica to be used in the
      # read operation.
      @consistency_level = opt[:consistency_level] || Aerospike::ConsistencyLevel::CONSISTENCY_ONE

      # Transaction timeout.
      # This timeout is used to set the socket timeout and is also sent to the
      # server along with the transaction in the wire protocol.
      # Default to no timeout (0).
      @timeout = opt[:timeout] || 0

      # Maximum number of retries before aborting the current transaction.
      # A retry is attempted when there is a network error other than timeout.
      # If max_retries is exceeded, the abort will occur even if the timeout
      # has not yet been exceeded.
      @max_retries = opt[:max_retiries] || 2

      # Duration to sleep between retries if a transaction fails and the
      # timeout was not exceeded. Enter zero to skip sleep.
      @sleep_between_retries = opt[:sleep_between_retries] || 0.5
    end


  end # class

end # module
