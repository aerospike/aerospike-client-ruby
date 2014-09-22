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

module Apik

  # Polymorphic value classes used to efficiently serialize objects into the wire protocol.
  class Info

    def self.request(conn, *commands)
      @buffer = Buffer.new

      # First, do quick conservative buffer size estimate.
      @offset = 8

      commands.each do |command|
        @offset += command.length * 2 + 1
      end

      # If conservative estimate may be exceeded, get exact estimate
      # to preserve memory and resize buffer.
      if (@offset > @buffer.length)
        @offset = 8

        commands.each do |command|
          @offset += command.length + 1
        end

        @buffer.resize(@offset)
      end

      @offset = 8 # Skip size field.

      # The command format is: <name1>\n<name2>\n...
      commands.each do |command|
        @buffer.write_binary(command, @offset)
        @offset += command.length
        @buffer.write_byte("\n", @offset)
        @offset += 1
      end

      sendCommand(conn)
      parse_multiple_response
    end

    def self.parse_multiple_response
      res = {}
      @buffer.buf.force_encoding('utf-8').split("\n").each do |vstr|
        k, v = vstr.split("\t")
        res[k] = v
      end

      res
    end

    private

    def self.sendCommand(conn)
      begin
        # Write size field.
        size = (@offset - 8) | (Integer(2) << 56) | (Integer(1) << 48)

        @buffer.write_int64(size, 0)

        # Write.
        conn.write(@buffer, @offset)

        # Read - reuse input buffer.
        conn.read(@buffer, 8)

        size = @buffer.read_int64(0)
        length = size & Integer(0xFFFFFFFFFFFF)

        @buffer.resize(length)
        conn.read(@buffer, length)
      rescue
        raise
      end
    end

  end

end
