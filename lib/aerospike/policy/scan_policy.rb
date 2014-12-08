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

require 'aerospike/policy/batch_policy'

module Aerospike

  # Container object for scan policy command.
  class ScanPolicy < BatchPolicy

    attr_accessor :scan_percent, :concurrent_nodes,
      :include_bin_data, :fail_on_cluster_change

    def initialize(scan_percent=nil, concurrent_nodes=nil, include_bin_data=nil, fail_on_cluster_change=nil)
      super()

      @scan_percent = scan_percent || 100
      @concurrent_nodes = concurrent_nodes.nil? ? true : concurrent_nodes
      @include_bin_data = include_bin_data.nil? ? true : include_bin_data
      @fail_on_cluster_change = fail_on_cluster_change.nil? ? true : fail_on_cluster_change

      @max_retries = 0

      self
    end

  end # class

end # module
