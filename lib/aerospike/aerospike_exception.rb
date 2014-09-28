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

module Aerospike

  module Exceptions

    class Aerospike < Exception

      attr_reader :result_code

      def initialize(result_code, message = nil)
        @result_code = result_code
        message ||= Aerospike::ResultCode.message(result_code)
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

        super(Aerospike::ResultCode::TIMEOUT)

      end

    end

    class Serialize < Aerospike

      def initialize(msg=nil)
        super(Aerospike::ResultCode::SERIALIZE_ERROR, msg)
      end

    end

    class Parse < Aerospike

      def initialize(msg=nil)
        super(Aerospike::ResultCode::PARSE_ERROR, msg)
      end

    end

    class Connection < Aerospike

      def initialize(msg=nil)
        super(Aerospike::ResultCode::SERVER_NOT_AVAILABLE, msg)
      end

    end

    class InvalidNode < Aerospike

      def initialize(msg=nil)
        super(Aerospike::ResultCode::INVALID_NODE_ERROR, msg)
      end

    end

    class ScanTerminated < Aerospike

      def initialize(msg=nil)
        super(Aerospike::ResultCode::SCAN_TERMINATED, msg)
      end

    end

    class QueryTerminated < Aerospike

      def initialize(msg=nil)
        super(Aerospike::ResultCode::QUERY_TERMINATED, msg)
      end

    end

    class CommandRejected < Aerospike

      def initialize(msg=nil)
        super(Aerospike::ResultCode::COMMAND_REJECTED, msg)
      end

    end

  end
end
