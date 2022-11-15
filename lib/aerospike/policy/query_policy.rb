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

  # Container object for query policy command.
  class QueryPolicy < Policy

    attr_accessor :concurrent_nodes
    attr_accessor :max_records
    attr_accessor :include_bin_data
    attr_accessor :record_queue_size
    attr_accessor :records_per_second
    attr_accessor :socket_timeout
    attr_accessor :short_query

    def initialize(opt={})
      super(opt)

      # Indicates if bin data is retrieved. If false, only record digests (and
      # user keys if stored on the server) are retrieved.
      # Default is true.
      @include_bin_data = opt.fetch(:include_bin_data, true)

      # Approximates the number of records to return to the client. This number is divided by the
      # number of nodes involved in the query. The actual number of records returned
      # may be less than MaxRecords if node record counts are small and unbalanced across
      # nodes.
      #
      # This field is supported on server versions >= 4.9.
      #
      # Default: 0 (do not limit record count)
      @max_records = opt.fetch(:max_records) { 0 }

      # Issue scan requests in parallel or serially.
      @concurrent_nodes = opt.fetch(:concurrent_nodes) { true }

      # Determines network timeout for each attempt.
      #
      # If socket_timeout is not zero and socket_timeout is reached before an attempt completes,
      # the Timeout above is checked. If Timeout is not exceeded, the transaction
      # is retried. If both socket_timeout and Timeout are non-zero, socket_timeout must be less
      # than or equal to Timeout, otherwise Timeout will also be used for socket_timeout.
      #
      # Default: 30s
      @socket_timeout = opt[:socket_timeout] || 30000

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

      # Detemine wether query expected to return less than 100 records.
      # If true, the server will optimize the query for a small record set.
      # This field is ignored for aggregation queries, background queries
      # and server versions 6.0+.
      #
      # Default: false
      @short_query = opt[:short_query] ||false

      self
    end

  end # class

end # module
