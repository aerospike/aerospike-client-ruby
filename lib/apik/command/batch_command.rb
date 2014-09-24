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

require 'thread'

require 'apik/record'

require 'apik/command/command'

module Apik

  protected

  class BatchCommand < Command

    def initialize(node)
      super(node)

      @valid = true
      @mutex = Mutex.new
      @records = Queue.new

      self
    end

    def parseResult
      # Read socket into receive buffer one record at a time.  Do not read entire receive size
      # because the receive buffer would be too big.
      status = true

      while status
        # Read header.
        readBytes(8)

        size = @dataBuffer.read_int64(0)
        receiveSize = size & 0xFFFFFFFFFFFF

        if receiveSize > 0
          status = parseRecordResults(receiveSize)
        else
          status = false
        end
      end
    end

    def parseKey(fieldCount)
      digest = nil
      namespace = nil
      setName = nil
      userKey = nil

      for i in 0...fieldCount
        readBytes(4)

        fieldlen = @dataBuffer.read_int32(0)
        readBytes(fieldlen)

        fieldtype = @dataBuffer.read(0).ord
        size = fieldlen - 1

        case fieldtype
        when Apik::FieldType::DIGEST_RIPE
          digest = @dataBuffer.read(1, size)
        when Apik::FieldType::NAMESPACE
          namespace = @dataBuffer.read(1, size).force_encoding('utf-8')
        when Apik::FieldType::TABLE
          setName = @dataBuffer.read(1, size).force_encoding('utf-8')
        when Apik::FieldType::KEY
          userKey = Value.bytesToKeyValue(@dataBuffer.read(1).ord, @dataBuffer, 2, size-1)
        end
      end

      Apik::Key.new(namespace, setName, userKey, digest)
    end

    def readBytes(length)
      if length > @dataBuffer.length
        # Corrupted data streams can result in a huge length.
        # Do a sanity check here.
        if length > Apik::Buffer::MAX_BUFFER_SIZE
          raise Apik::Exceptions::Parse.new("Invalid readBytes length: #{length}")
        end
        @dataBuffer = Buffer.new(length)
      end

      @conn.read(@dataBuffer, length)
      @dataOffset += length
    end

    def stop
      @mutex.synchronize do
        @valid = false
      end
    end

    def valid?
      res = nil
      @mutex.synchronize do
        res = @valid
      end

      res
    end

  end # class

end # module
