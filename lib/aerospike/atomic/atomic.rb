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

module Aerospike

  private

  # Container object for client policy command.
  class Atomic # :nodoc:

    def initialize(value)
      @value = value

      @mutex = Mutex.new
    end

    def update(&block)
      @mutex.synchronize do
        @value = block.call(@value)
      end
    end

    def get
      ret = nil
      @mutex.synchronize do
        ret = @value
      end
      ret
    end
    alias_method :value, :get
    alias_method :to_s, :value
    alias_method :inspect, :to_s

    def set(value)
      @mutex.synchronize do
        @value = value
      end
    end
    alias_method 'value='.to_sym, :set

  end # class

end # module
