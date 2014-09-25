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

require 'apik/record'

require 'apik/command/single_command'
require 'apik/utils/epoc'
require 'apik/value/value'

module Apik

  protected

  class ReadCommand < SingleCommand

    attr_reader :record

    def initialize(cluster, policy, key, binNames)
      super(cluster, key)

      @binNames = binNames
      @policy = policy

      self
    end

    def writeBuffer
      setRead(@key, @binNames)
    end

    def parseResult
      # Read header.
      # Logger.Debug("readCommand Parse Result: trying to read %d bytes from the connection...", int(_MSG_TOTAL_HEADER_SIZE))
      begin
        @conn.read(@dataBuffer, MSG_TOTAL_HEADER_SIZE)
      rescue Exception => e
        Apik.logger.warn("parse result error: #{e}")
        raise e
      end

      # A number of these are commented out because we just don't care enough to read
      # that section of the header. If we do care, uncomment and check!
      sz = @dataBuffer.read_int64(0)
      headerLength = @dataBuffer.read(8).ord
      resultCode = @dataBuffer.read(13).ord & 0xFF
      generation = @dataBuffer.read_int32(14)
      expiration = Apik.TTL(@dataBuffer.read_int32(18))
      fieldCount = @dataBuffer.read_int16(26) # almost certainly 0
      opCount = @dataBuffer.read_int16(28)
      receiveSize = (sz & 0xFFFFFFFFFFFF) - headerLength

      # Apik.logger.debug("readCommand Parse Result: resultCode: %d, headerLength: %d, generation: %d, expiration: %d, fieldCount: %d, opCount: %d, receiveSize: %d", resultCode, headerLength, generation, expiration, fieldCount, opCount, receiveSize)

      # Read remaining message bytes.
      if receiveSize > 0
        sizeBufferSz(receiveSize)

        begin
          @conn.read(@dataBuffer, receiveSize)
        rescue Exception => e
          Apik.logger.warn("parse result error: #{e}")
          raise e
        end

      end

      if resultCode != 0
        return if resultCode == Apik::ResultCode::KEY_NOT_FOUND_ERROR

        if resultCode == Apik::ResultCode::UDF_BAD_RESPONSE
          begin
            @record = parseRecord(opCount, fieldCount, generation, expiration)
            handleUdfError(resultCode)
          rescue Exception => e
            Apik.logger.warn("UDF execution error: #{e}")
            raise e
          end

        end

        raise Apik::Exceptions::Aerospike.new(resultCode)
      end

      if opCount == 0
        # data Bin was not returned.
        @record = Record.new(@node, @key, nil, nil, generation, expiration)
        return
      end

      @record = parseRecord(opCount, fieldCount, generation, expiration)
    end

    def handleUdfError(resultCode)
      ret = @record.bins['FAILURE']
      raise Apik::Exceptions::Aerospike.new(resultCode, ret) if ret
      raise Apik::Exceptions::Aerospike.new(resultCode)
    end

    def parseRecord(opCount, fieldCount, generation, expiration)
      bins = nil
      duplicates = nil
      receiveOffset = 0

      # There can be fields in the response (setname etc).
      # But for now, ignore them. Expose them to the API if needed in the future.
      # Logger.Debug("field count: %d, databuffer: %v", fieldCount, @dataBuffer)
      if fieldCount != 0
        # Just skip over all the fields
        for i in 0...fieldCount
          # Logger.Debug("%d", receiveOffset)
          fieldSize = @dataBuffer.read_int32(receiveOffset)
          receiveOffset += (4 + fieldSize)
        end
      end

      for i in 0...opCount
        opSize = @dataBuffer.read_int32(receiveOffset)
        particleType = @dataBuffer.read(receiveOffset+5).ord
        version = @dataBuffer.read(receiveOffset+6).ord
        nameSize = @dataBuffer.read(receiveOffset+7).ord
        name = @dataBuffer.read(receiveOffset+8, nameSize).force_encoding('utf-8')
        receiveOffset += 4 + 4 + nameSize


        particleBytesSize = opSize - (4 + nameSize)
        value = Apik.bytesToParticle(particleType, @dataBuffer, receiveOffset, particleBytesSize)
        receiveOffset += particleBytesSize

        vmap = {}

        if version > 0 || duplicates != nil
          unless duplicates
            duplicates = []
            duplicates << bins
            bins = nil

            for j in 0..version-1
              duplicates << nil
            end
          else
            for j in duplicates.length..version
              duplicates << nil
            end
          end

          vmap = duplicates[version]
          unless vmap
            vmap = {}
            duplicates[version] = vmap
          end
        else
          bins = {} unless bins
          vmap = bins
        end
        vmap[name] = value.get if value
      end

      # Remove nil duplicates just in case there were holes in the version number space.
      duplicates.compact! if duplicates

      Record.new(@node, @key, bins, duplicates, generation, expiration)
    end

  end # class

end # module
