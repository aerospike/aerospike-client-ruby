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

  # Recordset implements a queue for a producer-consumer pattern
  # a producer is a thread that fetches records from one node and puts them on this queue
  # a consumer fetches records from this queue
  # so the production and the consumptoin are decoupled
  # there can be an unlimited count of producer threads and consumer threads
  class Recordset

    attr_reader :records

    def initialize(queue_size = 5000, thread_count = 1, type)
      queue_size = thread_count if queue_size < thread_count
      @records = SizedQueue.new(queue_size)

      # holds the count of active threads.
      # when it reaches zero it means the whole operations of fetching records from server nodes is finished
      @active_threads = Atomic.new(thread_count)

      # operation cancelled by user or an exception occured in one of the threads
      @cancelled = Atomic.new(false)

      # saves the exception that occurred inside one of the threads to reraise it in the main thread
      # and also is a signal to terminate other threads as the whole operation is assumed as failed
      @thread_exception = Atomic.new(nil)

      # type of the operation. it is either :scan or :query
      @type = type
    end

    # fetches and return the first record from the queue
    # if the operation is not finished and the queue is empty it blocks and waits for new records
    # it sets the exception if it reaches the EOF mark, and returns nil
    # EOF means the operation has finished and no more records are comming from server nodes
    # it re-raises the exception occurred in threads, or which was set after reaching the EOF in the previous call
    def next_record
      raise @thread_exception.get unless @thread_exception.get.nil?

      r = @records.deq

      set_exception if r.nil?

      r
    end

    # recordset is active unless it is cancelled by the user or an exception has occurred in of threads
    def active?
      !@cancelled.get
    end

    # this is called by working threads to signal their job is finished
    # it decreases the count of active threads and puts an EOF on queue when all threads are finished
    def thread_finished
      @active_threads.update do |v|
        v -= 1
        @records.enq(nil) if v == 0
        v
      end
    end

    # this is called by a thread who faced an exception to singnal to terminate the whole operation
    # it also may be called by the user to terminate the command in the middle of fetching records from server nodes
    # it clears the queue so that if any threads are waiting for the queue get unblocked and find out about the cancellation
    def cancel(expn=nil)
      set_exception(expn)
      @cancelled.set(true)
      @records.clear
    end

    # fetches and returns all the records from the queue until the whole operation is finished and it reaches an EOF mark
    # calling cancel inside the each block raises an exception to signal other consumer threads
    def each(&block)
      r = true
      while r
        r = next_record
        # nil means EOF
        unless r.nil?
          block.call(r)
        else
          # reached the EOF
          break
        end
      end
    end

    def to_a
      records = []
      r = true
      while r
        r = next_record
        # nil means EOF
        unless r.nil?
          records << r
        else
          # reached the EOF
          break
        end
      end

      records
    end

    # the command is a scan if there are no filters applied otherwise it is a query
    def is_scan?
      @filters.nil? || @filters.empty?
    end

  private

    def set_exception(expn=nil)
      expn ||= (@type == :scan ? SCAN_TERMINATED_EXCEPTION : QUERY_TERMINATED_EXCEPTION)
      @thread_exception.set(expn)
    end

  end

  private

    SCAN_TERMINATED_EXCEPTION = Aerospike::Exceptions::ScanTerminated.new()
    QUERY_TERMINATED_EXCEPTION = Aerospike::Exceptions::QueryTerminated.new()

end
