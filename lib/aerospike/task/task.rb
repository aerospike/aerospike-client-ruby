# encoding: utf-8
# Copyright 2014 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require 'thread'
require 'time'
require 'atomic'

module Aerospike

  protected

  class Task

    def initialize(cluster, done)
      @cluster = cluster
      @done = Atomic.new(done)
      @done_thread = Atomic.new(nil)

      self
    end

    def wait_till_completed(poll_interval = 0.1, allowed_failures = 3)
      return true if @done.value

      # make sure there will be only ONE thread polling for completetion status
      @done_thread.update do |dt|
        dt ? dt : Thread.new do
          abort_on_exception=true
          failures = 0
          while true
            begin
              break if completed?
              sleep(poll_interval.to_f)
            rescue => e
              p e
              break if failures > allowed_failures
              failures += 1
            end
          end
        end
      end

      # wait for the poll thread to finish
      @done_thread.value.join
      # in case of errors and exceptions, the @done value might be false
      @done.value
    end

    def completed?
      if @done.value == true
        true
      else
        @done.value = all_nodes_done?
      end
    end

  end # class

end # module
