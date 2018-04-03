# frozen_string_literal: true

# Copyright 2018 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

module Aerospike
  class Node
    # generic class for representing changes in eg. peer and partition generation
    class Generation
      attr_reader :number

      def initialize(number = -1)
        @number = ::Aerospike::Atomic.new(number)
        @changed = ::Aerospike::Atomic.new(false)
      end

      def changed?
        @changed.value == true
      end

      def eql?(number)
        @number.value == number
      end

      def reset_changed!
        @changed.value = false
      end

      def update(new_number)
        return if @number.value == new_number
        @number.value = new_number
        @changed.value = true
      end
    end
  end
end
