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

  # Container object for batch policy command.
  class BatchPolicy < Policy

    attr_accessor :max_concurrent_nodes, :record_queue_size, 
      :wait_until_migrations_are_over, :use_batch_direct

    def initialize(opt={})
      super(opt)

      @max_concurrent_nodes = opt[:max_concurrent_nodes] || 0
      @record_queue_size = opt[:record_queue_size] || 5000
      @wait_until_migrations_are_over = opt[:wait_until_migrations_are_over].nil? ? false : wait_until_migrations_are_over
      @use_batch_direct = opt[:use_batch_direct].nil? ? false : opt[:use_batch_direct]
      self
    end

  end # class

end # module
