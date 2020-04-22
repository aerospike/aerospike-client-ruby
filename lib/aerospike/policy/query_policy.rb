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

  # Container object for query policy command.
  class QueryPolicy < Policy

    attr_accessor :include_bin_data
    attr_accessor :record_queue_size
    attr_accessor :records_per_second

    def initialize(opt={})
      super(opt)

      @max_retries = 0

      # Indicates if bin data is retrieved. If false, only record digests (and
      # user keys if stored on the server) are retrieved.
      # Default is true.
      @include_bin_data = opt.fetch(:include_bin_data, true)

      # Number of records to place in queue before blocking. Records received
      # from multiple server nodes will be placed in a queue. A separate thread
      # consumes these records in parallel. If the queue is full, the producer
      # threads will block until records are consumed.
      # Default is 5000.
      @record_queue_size = opt[:record_queue_size] || 5000

      # Limit returned records per second (rps) rate for each server.
      # Will not apply rps limit if records_per_second is zero.
      # Currently only applicable to a query without a defined filter (scan).
      # Default is 0
      @records_per_second = opt[:records_per_second] || 0

      self
    end

  end # class

end # module
