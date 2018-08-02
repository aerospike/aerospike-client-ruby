# frozen_string_literal: true
# Copyright 2014-2018 Aerospike, Inc.
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

  class Pool #:nodoc:

    attr_accessor :create_proc, :cleanup_proc, :check_proc

    def initialize(max_size = 256, &block)
      @create_proc = block
      @cleanup_proc = nil
      @check_proc = nil

      @pool = Queue.new
      @max_size = max_size
    end

    def offer(obj)
      if @pool.length < @max_size
        @pool << obj
      else
        cleanup(obj)
      end
    end
    alias_method :<<, :offer

    def poll(create_new=true)
      non_block = true
      begin
        obj = @pool.pop(non_block)
        if !check(obj)
          cleanup(obj)
          obj = nil
        end
      end until obj
      obj
    rescue ThreadError
      create if create_new
    end

    def empty?
      @pool.length == 0
    end

    def length
      @pool.length
    end
    alias_method :size, :length

    def inspect
      "#<Aerospike::Pool: size=#{size}>"
    end

    protected

    def create
      return unless create_proc
      create_proc.()
    end

    def check(obj)
      return true unless check_proc
      check_proc.(obj)
    end

    def cleanup(obj)
      return unless cleanup_proc
      cleanup_proc.(obj)
    end

  end

end
