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
        begin
          read_nonblock(length)
        rescue ::IO::WaitReadable => e
          if ::IO::select([self], nil, nil, @timeout)
            retry
          else
            raise ::Aerospike::Exceptions::Connection.new("#{e}")
          end
        rescue => e
          raise ::Aerospike::Exceptions::Connection.new("#{e}")
        end
      end

      def write(buffer, length)
        bytes_written = 0
        until bytes_written >= length
          bytes_written += write_to_socket(buffer.read(bytes_written, length - bytes_written))
        end
      end

      def write_to_socket(data)
        begin
          write_nonblock(data)
        rescue ::IO::WaitWritable => e
          if ::IO::select(nil, [self], nil, @timeout)
            retry
          else
            raise ::Aerospike::Exceptions::Connection.new("#{e}")
          end
        rescue => e
          raise ::Aerospike::Exceptions::Connection.new("#{e}")
        end
      end

      def timeout=(timeout)
        @timeout = timeout && timeout > 0 ? timeout : nil
      end

      def connected?
        !closed?
      end

      def close
        return if closed?
        super()
      end
    end
  end
end