# Copyright 2012-2014 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License") you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require 'apik/utils/buffer'

module Apik

  # Polymorphic value classes used to efficiently serialize objects into the wire protocol.
  class Info

    def self.request(conn, *commands)
      buffer = Buffer.get

      # If conservative estimate may be exceeded, get exact estimate
      # to preserve memory and resize buffer.
      offset = 8

      commands.each do |command|
        offset += command.bytesize + 1
      end

      buffer.resize(offset)

      offset = 8 # Skip size field.

      # The command format is: <name1>\n<name2>\n...
      commands.each do |command|
        buffer.write_binary(command, offset)
        offset += command.bytesize
        buffer.write_byte("\n", offset)
        offset += 1
      end

      buf_length = send_command(conn, offset, buffer)
      res = parse_multiple_response(buf_length, buffer)
      Buffer.put(buffer)
      res
    end

    def self.parse_multiple_response(buf_length, buffer)
      res = {}
      buffer.read(0, buf_length).split("\n").each do |vstr|
        k, v = vstr.split("\t")
        res[k] = v
      end
      res
    end

    private

    def self.send_command(conn, offset, buffer)
      begin
        # Write size field.
        size = (offset - 8) | (2 << 56) | (1 << 48)

        buffer.write_int64(size, 0)
        conn.write(buffer, offset)

        # Read - reuse input buffer.
        conn.read(buffer, 8)

        size = buffer.read_int64(0)
        length = size & 0xFFFFFFFFFFFF

        buffer.resize(length)

        conn.read(buffer, length)
        return length
      rescue Exception => e
        conn.close
        raise e
      end
    end

  end

end
