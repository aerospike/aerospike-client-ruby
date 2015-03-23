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

require 'socket'

module Aerospike

  private

  class Connection # :nodoc:

    def initialize(host, port, timeout = 30)

      connect(host, port, timeout).tap do |socket|
        @socket = socket
        @timeout = timeout
      end

      self
    end

    def connect(host, port, timeout)
      socket = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
      @sockaddr = Socket.sockaddr_in(port, host)
      begin
        socket.connect_nonblock(@sockaddr)
        socket
      rescue IO::WaitWritable
        # Block until the socket is ready, then try again
        IO.select(nil, [socket], nil, timeout.to_f)
        begin
          socket.connect_nonblock(@sockaddr)
        rescue Errno::EISCONN
        end
        socket
      end
    end

    def write(buffer, length)
      total = 0
      while total < length
        begin
          written = @socket.write_nonblock(buffer.read(total, length - total))
          total += written
        rescue IO::WaitWritable, Errno::EAGAIN
          IO.select(nil, [@socket])
          retry
        rescue => e
          Aerospike::Exceptions::Connection.new("#{e}")
        end
      end
    end

    def read(buffer, length)
      total = 0
      while total < length
        begin
          bytes = @socket.recv_nonblock(length - total)
          buffer.write_binary(bytes, total) if bytes.bytesize > 0
          total += bytes.bytesize
        rescue IO::WaitReadable,  Errno::EAGAIN
          IO.select([@socket], nil)
          retry
        rescue => e
          Aerospike::Exceptions::Connection.new("#{e}")
        end
      end
    end

    def connected?
      @socket != nil
    end

    def valid?
      @socket != nil
    end

    def close
      @socket.close if @socket
      @socket = nil
    end

    def timeout=(timeout)
      if timeout > 0 && timeout != @timeout
        @timeout = timeout
        if IO.select([@socket], [@socket], [@socket], timeout.to_f)
          begin
            # Verify there is now a good connection
            @socket.connect_nonblock(@sockaddr)
          rescue Errno::EISCONN
            # operation successful
          rescue => e
            # An unexpected exception was raised - the connection is no good.
            close
            raise Aerospike::Exceptions::Connection.new("#{e}")
          end
        end
      end
    end

  end

end
