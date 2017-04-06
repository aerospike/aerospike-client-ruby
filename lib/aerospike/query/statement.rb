# encoding: utf-8
# Copyright 2014-2017 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

module Aerospike

  class Statement

    attr_accessor :namespace, :set_name, :index_name, :bin_names, :task_id
    attr_accessor :filters, :package_name, :function_name, :function_args
    attr_accessor :return_data

    def initialize(namespace, set_name, bin_names=[])
      # Namespace determines query Namespace
      @namespace = namespace

      # SetName determines query Set name (Optional)
      @set_name = set_name

      # IndexName determines query index name (Optional)
      # If not set, the server will determine the index from the filter's bin name.
      @index_name = nil

      # BinNames detemines bin names (optional)
      @bin_names = bin_names

      # Filters determine query filters (Optional)
      # Currently, only one filter is allowed by the server on a secondary index lookup.
      # If multiple filters are necessary, see QueryFilter example for a workaround.
      # QueryFilter demonstrates how to add additional filters in an user-defined
      # aggregation function.
      @filters = []

      @package_name  = nil
      @function_name = nil
      @function_args = nil

      # TaskId determines query task id. (Optional)
      @task_id = rand(RAND_MAX)

      # determines if the query should return data
      @return_data = true
    end

  def set_aggregate_function(package_name, function_name, function_args=[], return_data=true)
      @package_name  = package_name
      @function_name = function_name
      @function_args = function_args
      @return_data = return_data
  end

  def is_scan?
    return (filters.nil? || (filters.empty?))
  end

  def set_task_id
    while @task_id == 0
      @task_id = rand(RAND_MAX)
    end
  end

  private

  RAND_MAX = 2**63

  end # class

end
