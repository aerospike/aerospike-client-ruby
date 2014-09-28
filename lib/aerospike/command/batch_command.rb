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

require 'thread'

require 'aerospike/record'

require 'aerospike/command/command'

module Aerospike

  protected

  class BatchCommand < Command

    def initialize(node)
      super(node)

      @valid = true
      @mutex = Mutex.new
      @records = Queue.new

      self
    end

    def parse_result
      # Read socket into receive buffer one record at a time.  Do not read entire receive size
      # because the receive buffer would be too big.
      status = true

      while status
        # Read header.
        read_bytes(8)

        size = @data_buffer.read_int64(0)
        receive_size = size & 0xFFFFFFFFFFFF

        if receive_size > 0
          status = parse_record_results(receive_size)
        else
          status = false
        end
      end
    end

    def parse_key(field_count)
      digest = nil
      namespace = nil
      set_name = nil
      user_key = nil

      for i in 0...field_count
        read_bytes(4)

        fieldlen = @data_buffer.read_int32(0)
        read_bytes(fieldlen)

        fieldtype = @data_buffer.read(0).ord
        size = fieldlen - 1

        case fieldtype
        when Aerospike::FieldType::DIGEST_RIPE
          digest = @data_buffer.read(1, size)
        when Aerospike::FieldType::NAMESPACE
          namespace = @data_buffer.read(1, size).force_encoding('utf-8')
        when Aerospike::FieldType::TABLE
          set_name = @data_buffer.read(1, size).force_encoding('utf-8')
        when Aerospike::FieldType::KEY
          user_key = Value.bytes_to_key_value(@data_buffer.read(1).ord, @data_buffer, 2, size-1)
        end
      end

      Aerospike::Key.new(namespace, set_name, user_key, digest)
    end

    def read_bytes(length)
      if length > @data_buffer.length
        # Corrupted data streams can result in a huge length.
        # Do a sanity check here.
        if length > Aerospike::Buffer::MAX_BUFFER_SIZE
          raise Aerospike::Exceptions::Parse.new("Invalid read_bytes length: #{length}")
        end
        @data_buffer = Buffer.new(length)
      end

      @conn.read(@data_buffer, length)
      @data_offset += length
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
