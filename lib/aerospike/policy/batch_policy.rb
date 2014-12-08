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

require 'aerospike/policy/policy'

module Aerospike

  # Container object for batch policy command.
  class BatchPolicy < Policy

    attr_accessor :max_concurrent_nodes, :record_queue_size, 
      :wait_until_migrations_are_over

    def initialize(max_concurrent_nodes=nil, record_queue_size=nil, wait_until_migrations_are_over=nil)
      super()

      @max_concurrent_nodes = max_concurrent_nodes || 0
      @record_queue_size = record_queue_size || 5000
      @wait_until_migrations_are_over = wait_until_migrations_are_over.nil? ? false : wait_until_migrations_are_over

      self
    end

  end # class

end # module
