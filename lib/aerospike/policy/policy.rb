# encoding: utf-8
# Copyright 2014-2017 Aerospike, Inc.
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

    attr_accessor :priority, :timeout, :max_retries, :sleep_between_retries, :consistency_level,
                  :predexp, :fail_on_filtered_out

    def initialize(opt={})
      # Container object for transaction policy attributes used in all database
      # operation calls.

      # Priority of request relative to other transactions.
      # Currently, only used for scans.
      @priority = opt[:priority] || Priority::DEFAULT

      # Set optional predicate expression filters in postfix notation.
      # Predicate expression filters are applied on the query results on the server.
      # Predicate expression filters may occur on any bin in the record.
      # Requires Aerospike Server versions >= 3.12
      #
      # Postfix notation is described here: http://wiki.c2.com/?PostfixNotation
      #
      # Example:
      #
      # (c >= 11 and c <= 20) or (d > 3 and (d < 5)
      # policy.predexp = [
      #   PredExp.integer_bin("c"),
      #   PredExp.integer_value(11),
      #   PredExp.integer_greater_eq(),
      #   PredExp.integer_bin("c"),
      #   PredExp.integer_value(20),
      #   PredExp.integer_less_eq(),
      #   PredExp.and(2),
      #   PredExp.integer_bin("d"),
      #   PredExp.integer_value(3),
      #   PredExp.integer_greater(),
      #   PredExp.integer_bin("d"),
      #   PredExp.integer_value(5),
      #   PredExp.integer_less(),
      #   PredExp.and(2),
      #   PredExp.or(2)
      # ]
      #
      # # Record last update time > 2017-01-15
      # policy.predexp = [
      #   PredExp.rec_last_update(),
      #   PredExp.integer_value(Time.new(2017, 1, 15).to_i),
      #   PredExp.integer_greater(),
      #   PredExp.integer_greater()
      # ]
      @predexp = opt[:predexp] || nil


      # Throw exception if @predexp is defined and that filter evaluates
      # to false (transaction ignored). The Aerospike::Exceptions::Aerospike
      # will contain result code Aerospike::ResultCode::FILTERED_OUT.
      # This field is not applicable to batch, scan or query commands.
      @fail_on_filtered_out = opt[:fail_on_filtered_out] || false

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
      @max_retries = opt[:max_retries] || 2

      # Duration to sleep between retries if a transaction fails and the
      # timeout was not exceeded. Enter zero to skip sleep.
      @sleep_between_retries = opt[:sleep_between_retries] || 0.5
    end


  end # class

end # module
