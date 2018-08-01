# frozen_string_literal: true

# Copyright 2014-2018 Aerospike, Inc.
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
  module Socket
    module Base
      def initialize(*args)
        super(*args)
        @timeout = nil
      end

      def read(buffer, length)
        bytes_read = 0
        until bytes_read >= length
          result = read_from_socket(length - bytes_read)
          buffer.write_binary(result, bytes_read)
          bytes_read += result.bytesize
        end
      end

      def read_from_socket(length)
        with_timeout(@timeout) do
          read_nonblock(length)
        end
      end

      def write(buffer, length)
        bytes_written = 0
        until bytes_written >= length
          bytes_written += write_to_socket(buffer.read(bytes_written, length - bytes_written))
        end
      end

      def write_to_socket(data)
        with_timeout(@timeout) do
          write_nonblock(data)
        end
      end

      def timeout=(timeout)
        @timeout = timeout && timeout > 0 ? timeout : nil
      end

      def connected?
        !closed?
      end

      # Returns whether the connection to the server is alive.
      #
      # It is useful to call this method before making a call to the server
      # that would change data on the server.
      #
      # Note: This method is only useful if the server closed the connection or
      # if a previous connection failure occurred. If the server is hard killed
      # this will still return true until one or more writes are attempted.
      def alive?
        return false if closed?

        if IO.select([self], nil, nil, 0)
          !eof? rescue false
        else
          true
        end
      rescue IOError
        false
      end

      def close
        return if closed?
        super()
      end

      private

      # Note: For SSL connections, read_nonblock may invoke write system call,
      # which may raise IO::WaitWritable, and vice versa, due to SSL
      # renegotiation, so we should always rescue both.
      def with_timeout(timeout, &block)
        block.call
      rescue IO::WaitReadable => e
        if IO::select([self], nil, nil, timeout)
          retry
        else
          fail Aerospike::Exceptions::Connection, "Socket timeout: #{e}"
        end
      rescue IO::WaitWritable => e
        if IO::select(nil, [self], nil, timeout)
          retry
        else
          fail Aerospike::Exceptions::Connection, "Socket timeout: #{e}"
        end
      rescue => e
        raise Aerospike::Exceptions::Connection, "Socket error: #{e}"
      end
    end
  end
end