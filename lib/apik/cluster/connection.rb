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

require 'socket'

module Apik

  class Connection

    def initialize(host, port, timeout = 30)

      # Convert the passed host into structures the non-blocking calls
      # can deal with
      # addr = Socket.getaddrinfo(host, nil)
      # p addr[0][2]
      # sockaddr = Socket.pack_sockaddr_in(port, addr[0][2])
      # p sockaddr

      socket = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
      sockaddr = Socket.sockaddr_in(port, host)

      begin
        # Initiate the socket connection in the background. If it doesn't fail
        # immediately it will raise an IO::WaitWritable (Errno::EINPROGRESS)
        # indicating the connection is in progress.
        socket.connect_nonblock(sockaddr)

      rescue Errno::EINPROGRESS, IO::EINPROGRESSWaitWritable
        # IO.select will block until the socket is writable or the timeout
        # is exceeded - whichever comes first.
        reader, writer, errors =  IO.select([socket], [socket], [socket], timeout.to_f)
        @socket = socket
        @sockaddr = sockaddr
      else
        # IO.select returns nil when the socket is not ready before timeout
        # seconds have elapsed
        socket.close
        raise
      end

      self
    end

    def write(buffer, length)
      # TODO: COmplete implemetation
      begin
        @socket.write buffer.read(0, length)
        #rescue
      end
    end

    def read(buffer, length)
      # TODO: Complete implemetation
      total = 0
      while total < length
        begin
          #bytes = @socket.recv_nonblock(length)
          bytes = @socket.recv(length - total)
          buffer.write_binary(bytes, total)
          total += bytes.bytesize
        rescue IO::EAGAINWaitReadable
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
      @socket.close
    end

    def set_timeout(timeout = 5)
      if IO.select(nil, [@socket], nil, timeout.to_f)
        begin
          # Verify there is now a good connection
          @socket.connect_nonblock(@sockaddr)
        rescue Errno::EISCONN
          # operation successful
        rescue
          # An unexpected exception was raised - the connection is no good.
          @socket.close
          raise
        end
      end
    end

  end

end
