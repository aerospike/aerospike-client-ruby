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

require 'aerospike/command/single_command'

module Aerospike

  private

  class TouchCommand < SingleCommand

    def initialize(cluster, policy, key)
      super(cluster, key)

      @policy = policy

      self
    end

    def write_buffer
      set_touch(@policy, @key)
    end

    def parse_result
      # Read header.
      @conn.read(@data_buffer, MSG_TOTAL_HEADER_SIZE)

      result_code = @data_buffer.read(13).ord & 0xFF

      raise Aerospike::Exceptions::Aerospike.new(result_code) if result_code != 0

      empty_socket
    end

  end # class

end # module
