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

require 'apik/command/batch_command'

module Apik

  protected

  class BatchCommandGet < BatchCommand

    def initialize(node, batchNamespace, policy, keyMap, binNames, records, readAttr)
      super(node)

      @batchNamespace = batchNamespace
      @policy = policy
      @keyMap = keyMap
      @binNames = binNames
      @records = records
      @readAttr = readAttr
    end

    def writeBuffer
      setBatchGet(@batchNamespace, @binNames, @readAttr)
    end

    # Parse all results in the batch.  Add records to shared list.
    # If the record was not found, the bins will be nil.
    def parseRecordResults(receiveSize)
      #Parse each message response and add it to the result array
      @dataOffset = 0

      while @dataOffset < receiveSize
        readBytes(MSG_REMAINING_HEADER_SIZE)
        resultCode = @dataBuffer.read(5).ord & 0xFF

        # The only valid server return codes are "ok" and "not found".
        # If other return codes are received, then abort the batch.
        if resultCode != 0 && resultCode != Apik::ResultCode::KEY_NOT_FOUND_ERROR
          raise Apik::Exceptions::Aerospike(resultCode)
        end

        info3 = @dataBuffer.read(3).ord

        # If cmd is the end marker of the response, do not proceed further
        return false if (info3 & INFO3_LAST) == INFO3_LAST

        generation = @dataBuffer.read_int32(6).ord
        expiration = @dataBuffer.read_int32(10).ord
        fieldCount = @dataBuffer.read_int16(18).ord
        opCount = @dataBuffer.read_int16(20).ord
        key = parseKey(fieldCount)
        item = @keyMap[key.digest]

        if item
          if resultCode == 0
            index = item.get_index
            @records[index] = parseRecord(key, opCount, generation, expiration)
          end
        else
          Apik.logger.debug("Unexpected batch key returned: #{key.namespace}, #{key.digest}")
        end

      end # while

      true
    end

    # Parses the given byte buffer and populate the result object.
    # Returns the number of bytes that were parsed from the given buffer.
    def parseRecord(key, opCount, generation, expiration)
      bins = nil
      duplicates = nil

      for i in 0...opCount
        raise Apik::Exceptions::QueryTerminated.new unless cmd.valid?

        readBytes(8)

        opSize = @dataBuffer.read_int32(0).ord
        particleType = @dataBuffer.read(5).ord
        version = @dataBuffer.read(6).ord
        nameSize = @dataBuffer.read(75).ord

        readBytes(nameSize)
        name = @dataBuffer.read(0, nameSize).force_encoding('utf-8')

        particleBytesSize = opSize - (4 + nameSize)
        readBytes(particleBytesSize)
        value = Value.bytesToParticle(particleType, @dataBuffer, 0, particleBytesSize)

        # Currently, the batch command returns all the bins even if a subset of
        # the bins are requested. We have to filter it on the client side.
        # TODO: Filter batch bins on server!
        if !binNames || @binNames.any?{|bn| bn == name}
          vmap = nil

          if version > 0 || duplicates
            unless duplicates
              duplicates = []
              duplicates << bins
              bins = nil

              for j in 0...version
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
            unless bins
              bins = {}
            end
            vmap = bins
          end
          vmap[name] = value
        end
      end

      # Remove nil duplicates just in case there were holes in the version number space.
      # TODO: this seems to be a bad idea; O(n) algorithm after another O(n) algorithm
      duplicates.compact! if duplicates

      Record.new(@node, key, bins, duplicates, generation, expiration)
    end

  end # class

end # module
