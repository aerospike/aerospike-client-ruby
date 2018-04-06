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

require 'socket'

module Aerospike
  module Socket
    class TCP < ::Socket
      include Base

      def self.connect(host, port, timeout)
        Aerospike.logger.debug("Trying to connect to #{host}:#{port} with #{timeout}s timeout")

        domain = if host.match(Resolv::IPv6::Regex)
          ::Socket::AF_INET6
        else
          ::Socket::AF_INET
        end

        sock = new(domain, ::Socket::SOCK_STREAM, 0)
        sockaddr = ::Socket.sockaddr_in(port, host)

        begin
          sock.connect_nonblock(sockaddr)
        rescue IO::WaitWritable, Errno::EINPROGRESS
          ::IO.select(nil, [sock], nil, timeout)

          # Because IO.select behaves (return values are different) differently on
          # different rubies, lets just try `connect_noblock` again. An exception
          # is raised to indicate the current state of the connection, and at this
          # point, we are ready to decide if this is a success or a timeout.
          begin
            sock.connect_nonblock(sockaddr)
          rescue Errno::EISCONN
            # Good, we're connected.
          rescue Errno::EINPROGRESS, Errno::EALREADY
            # Bad, we're still waiting to connect.
            raise ::Aerospike::Exceptions::Connection, "Connection attempt to #{host}:#{port} timed out after #{timeout} secs"
          rescue => e
            raise ::Aerospike::Exceptions::Connection, e.message
          end
        end

        sock
      end
    end
  end
end
