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

require 'aerospike/policy/policy'

module Aerospike

  # Container object for scan policy command.
  class ScanPolicy < Policy

    attr_accessor :scan_percent
    attr_accessor :concurrent_nodes
    attr_accessor :include_bin_data
    attr_accessor :fail_on_cluster_change
    attr_accessor :socket_timeout
    attr_accessor :record_queue_size

    def initialize(opt={})
      super(opt)

      @max_retries = 0

      # Percent of data to scan. Valid integer range is 1 to 100.
      # Default is 100.
      @scan_percent = opt[:scan_percent] || 100

      # Issue scan requests in parallel or serially.
      @concurrent_nodes = opt.fetch(:concurrent_nodes) { true }

      # Indicates if bin data is retrieved. If false, only record digests (and
      # user keys if stored on the server) are retrieved.
      # Default is true.
      @include_bin_data = opt.fetch(:include_bin_data) { true }

      # Terminate scan if cluster in fluctuating state.
      # Default is true.
      @fail_on_cluster_change = opt.fetch(:fail_on_cluster_change) { true }

      @socket_timeout = opt[:socket_timeout] || 10000

      # Number of records to place in queue before blocking. Records received
      # from multiple server nodes will be placed in a queue. A separate thread
      # consumes these records in parallel. If the queue is full, the producer
      # threads will block until records are consumed.
      # Default is 5000.
      @record_queue_size = opt[:record_queue_size] || 5000

      self
    end

  end # class

end # module
