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

module Apik

  module Exceptions

    class Aerospike < Exception

      attr_reader :result_code

      def initialize(result_code, message = nil)
        @result_code = result_code
        message ||= Apik::ResultCode.message(result_code)
        super(message)

        self
      end

    end

    class Timeout < Aerospike

      attr_reader :timeout, :iterations, :failed_nodes, :failed_connections

      def initialize(timeout, iterations, failed_nodes=nil, failed_connections=nil)

        @timeout = timeout
        @iterations = iterations
        @failed_nodes = failed_nodes
        @failed_connections = failed_connections

        super(Apik::ResultCode::TIMEOUT)

      end

    end

    class Serialize < Aerospike

      def initialize
        super(Apik::ResultCode::SERIALIZE_ERROR)
      end

    end

    class Parse < Aerospike

      def initialize
        super(Apik::ResultCode::PARSE_ERROR)
      end

    end

    class Connection < Aerospike

      def initialize
        super(Apik::ResultCode::SERVER_NOT_AVAILABLE)
      end

    end

    class InvalidNode < Aerospike

      def initialize
        super(Apik::ResultCode::INVALID_NODE_ERROR)
      end

    end

    class ScanTerminated < Aerospike

      def initialize
        super(Apik::ResultCode::SCAN_TERMINATED)
      end

    end

    class QueryTerminated < Aerospike

      def initialize
        super(Apik::ResultCode::QUERY_TERMINATED)
      end

    end

    class CommandRejected < Aerospike

      def initialize
        super(Apik::ResultCode::COMMAND_REJECTED)
      end

    end

  end
end
