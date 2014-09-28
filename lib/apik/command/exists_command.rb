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

require 'apik/command/single_command'
require 'apik/result_code'

module Apik

  protected

  class ExistsCommand < SingleCommand

    attr_reader :exists

    def initialize(cluster, policy, key)
      super(cluster, key)

      @policy = policy

      self
    end

    def write_buffer
      set_exists(@key)
    end

    def parse_result
      # Read header.
      @conn.read(@data_buffer, MSG_TOTAL_HEADER_SIZE)

      result_code = @data_buffer.read(13).ord & 0xFF

      if (result_code != 0) && (result_code != Apik::ResultCode::KEY_NOT_FOUND_ERROR)
        raise Apik::Exceptions::Aerospike.new(result_code)
      end

      @exists = (result_code == 0)

      empty_socket
    end

  end # class

end # module
