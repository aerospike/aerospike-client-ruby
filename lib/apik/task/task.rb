# Copyright 2012-2014 Aerospike, Inc.
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

module Apik

  protected

  class Task

    def initialize(cluster, done)
      @cluster = cluster
      @done = Atomic.new(done)
      @mutex = Mutex.new
      @done_event = ConditionVariable.new
      @done_thread = nil

      self
    end

    def wait_till_completed(poll_interval = 0.1, allowed_failures = 3)
      unless @done_thread
        @mutex.synchronize do
          @done_thread = Thread.new do
            failures = 0
            while true
              begin
                break if completed?
                sleep(poll_interval.to_f)
              rescue
                failures += 1
                if failures > allowed_failures
                  @done_event.broadcast
                  break
                end
              end
            end
          end

          @done_event.wait(@mutex)
          @done.value
        end
      end
    end

    def completed?
      @done.value ? @done.value : all_nodes_done?
    end

  end # class

end # module
