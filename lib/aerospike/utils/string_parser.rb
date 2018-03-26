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
  module Utils
    class StringParser
      attr_reader :io
      def initialize(str)
        @io = ::StringIO.new(str)
      end

      def current
        @io.string[@io.tell]
      end

      # Reads next character and raise if not matching desired one
      def expect(char)
        raise ::Aerospike::Exceptions::Parse unless @io.read(1) == char
      end

      def read_until(char)
        [].tap do |result|
          loop do
            chr = @io.read(1)
            break if chr == char
            result << chr
          end
        end.join
      end

      def step(count = 1)
        @io.read(count)
      end

    end
  end
end
