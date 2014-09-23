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

  class DeleteCommand < SingleCommand

    attr_reader :existed

    def initialize(cluster, policy, key)
      super(cluster, key)

      @policy = policy

      self
    end

    def writeBuffer
      setDelete(@policy, @key)
    end

    def parseResult
      # Read header.
      @conn.read(@dataBuffer, MSG_TOTAL_HEADER_SIZE)

      resultCode = @dataBuffer.read(13).ord & 0xFF

      if (resultCode != 0) && (resultCode != Apik::ResultCode::KEY_NOT_FOUND_ERROR)
        raise Apik::Exceptions::Aerospike.new(resultCode)
      end

      @existed = (resultCode == 0)

      emptySocket
    end

  end # class

end # module
