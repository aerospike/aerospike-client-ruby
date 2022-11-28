# frozen_string_literal: true

# Copyright 2014-2020 Aerospike, Inc.
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

module Aerospike
  class ScanExecutor # :nodoc:
    def self.scan_partitions(policy, cluster, tracker, namespace, set_name, recordset, bin_names = nil)
      interval = policy.sleep_between_retries

      should_retry = false

      loop do
        # reset last_expn
        @last_expn = nil

        list = tracker.assign_partitions_to_nodes(cluster, namespace)

        if policy.concurrent_nodes
          threads = []
          # Use a thread per node
          list.each do |node_partition|
            threads << Thread.new do
              Thread.current.abort_on_exception = true
              command = ScanPartitionCommand.new(policy, tracker, node_partition, namespace, set_name, bin_names, recordset)
              begin
                command.execute
              rescue => e
                @last_expn = e unless e == SCAN_TERMINATED_EXCEPTION
                should_retry ||= command.should_retry(e)
                Aerospike.logger.error(e.backtrace.join("\n")) unless e == SCAN_TERMINATED_EXCEPTION
              end
            end
          end
          threads.each(&:join)
        else
          # Use a single thread for all nodes for all node
          list.each do |node_partition|
            command = ScanPartitionCommand.new(policy, tracker, node_partition, namespace, set_name, bin_names, recordset)
            begin
              command.execute
            rescue => e
              @last_expn = e unless e == SCAN_TERMINATED_EXCEPTION
              should_retry ||= command.should_retry(e)
              Aerospike.logger.error(e.backtrace.join("\n")) unless e == SCAN_TERMINATED_EXCEPTION
            end
          end
        end

        if tracker.complete?(@cluster, policy) || !should_retry
          recordset.thread_finished(@last_expn)
          return
        end
        sleep(interval) if policy.sleep_between_retries > 0
        statement.reset_task_id
      end
    end
  end
end
