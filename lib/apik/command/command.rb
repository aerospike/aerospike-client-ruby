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

require 'time'

require 'apik/result_code'
require 'apik/command/field_type'

module Apik

  protected

  # Flags commented out are not supported by cmd client.
  # Contains a read operation.
  INFO1_READ = Integer(1 << 0)
  # Get all bins.
  INFO1_GET_ALL = Integer(1 << 1)

  # Do not read the bins
  INFO1_NOBINDATA = Integer(1 << 5)

  # Create or update record
  INFO2_WRITE = Integer(1 << 0)
  # Fling a record into the belly of Moloch.
  INFO2_DELETE = Integer(1 << 1)
  # Update if expected generation == old.
  INFO2_GENERATION = Integer(1 << 2)
  # Update if new generation >= old, good for restore.
  INFO2_GENERATION_GT = Integer(1 << 3)
  # Create a duplicate on a generation collision.
  INFO2_GENERATION_DUP = Integer(1 << 4)
  # Create only. Fail if record already exists.
  INFO2_CREATE_ONLY = Integer(1 << 5)

  # This is the last of a multi-part message.
  INFO3_LAST = Integer(1 << 0)
  # Update only. Merge bins.
  INFO3_UPDATE_ONLY = Integer(1 << 3)

  # Create or completely replace record.
  INFO3_CREATE_OR_REPLACE = Integer(1 << 4)
  # Completely replace existing record only.
  INFO3_REPLACE_ONLY = Integer(1 << 5)

  MSG_TOTAL_HEADER_SIZE      = 30
  FIELD_HEADER_SIZE          = 5
  OPERATION_HEADER_SIZE      = 8
  MSG_REMAINING_HEADER_SIZE  = 22
  DIGEST_SIZE                = 20
  CL_MSG_VERSION             = 2
  AS_MSG_TYPE                = 3

  class Command

    def initialize(node)
      @node = node

      self
    end

    # Writes the command for write operations
    def setWrite(policy, operation, key, bins)
      begin_cmd
      fieldCount = estimateKeySize(key)

      if policy.SendKey
        # field header size + key size
        @dataOffset += key.userKeyAsValue.estimateSize + FIELD_HEADER_SIZE
        fieldCount += 1
      end

      bins.each do |bin|
        estimateOperationSizeForBin(bin)
      end

      sizeBuffer

      writeHeaderWithPolicy(policy, 0, INFO2_WRITE, fieldCount, bins.length)
      writeKey(key)

      if policy.SendKey
        writeFieldValue(key.userKeyAsValue, Apik::FieldType::KEY)
      end

      bins.each do |bin|
        writeOperationForBin(bin, operation)
      end

      end_cmd
    end

    # Writes the command for delete operations
    def setDelete(policy, key)
      begin_cmd
      fieldCount = estimateKeySize(key)
      sizeBuffer
      writeHeaderWithPolicy(policy, 0, INFO2_WRITE|INFO2_DELETE, fieldCount, 0)
      writeKey(key)
      end_cmd
    end

    # Writes the command for touch operations
    def setTouch(policy, key)
      begin_cmd
      fieldCount = estimateKeySize(key)
      estimateOperationSize
      sizeBuffer
      writeHeaderWithPolicy(policy, 0, INFO2_WRITE, fieldCount, 1)
      writeKey(key)
      writeOperationForOperationType(Apik::Operation::TOUCH)
      end_cmd
    end

    # Writes the command for exist operations
    def setExists(key)
      begin_cmd
      fieldCount = estimateKeySize(key)
      sizeBuffer
      writeHeader(INFO1_READ|INFO1_NOBINDATA, 0, fieldCount, 0)
      writeKey(key)
      end_cmd
    end

    # Writes the command for get operations (all bins)
    def setReadForKeyOnly(key)
      begin_cmd
      fieldCount = estimateKeySize(key)
      sizeBuffer
      writeHeader(INFO1_READ|INFO1_GET_ALL, 0, fieldCount, 0)
      writeKey(key)
      end_cmd
    end

    # Writes the command for get operations (specified bins)
    def setRead(key, binNames)
      if binNames && binNames.length > 0
        begin_cmd
        fieldCount = estimateKeySize(key)

        binNames.each do |binName|
          estimateOperationSizeForBinName(binName)
        end

        sizeBuffer
        writeHeader(INFO1_READ, 0, fieldCount, binNames.length)
        writeKey(key)

        binNames.each do |binName|
          writeOperationForBinName(binName, Apik::Operation::READ)
        end

        end_cmd
      else
        setReadForKeyOnly(key)
      end
    end

    # Writes the command for getting metadata operations
    def setReadHeader(key)
      begin_cmd
      fieldCount = estimateKeySize(key)
      estimateOperationSizeForBinName('')
      sizeBuffer

      # The server does not currently return record header data with _INFO1_NOBINDATA attribute set.
      # The workaround is to request a non-existent bin.
      # TODO: Fix this on server.
      #command.setRead(INFO1_READ | _INFO1_NOBINDATA);
      writeHeader(INFO1_READ, 0, fieldCount, 1)

      writeKey(key)
      writeOperationForBinName('', Apik::Operation::READ)
      end_cmd
    end

    # Implements different command operations
    def setOperate(policy, key, operations)
      begin_cmd
      fieldCount = estimateKeySize(key)
      readAttr = 0
      writeAttr = 0
      readHeader = false

      operations.each do |operation|
        case operation.op_type
        when Apik::Operation::READ
            readAttr |= INFO1_READ

          # Read all bins if no bin is specified.
          readAttr |= INFO1_GET_ALL unless operation.bin_name

        when Apik::Operation::READ_HEADER
            # The server does not currently return record header data with _INFO1_NOBINDATA attribute set.
            # The workaround is to request a non-existent bin.
            # TODO: Fix this on server.
            # readAttr |= _INFO1_READ | _INFO1_NOBINDATA
            readAttr |= INFO1_READ
          readHeader = true

        else
          writeAttr = INFO2_WRITE
        end

        estimateOperationSizeForOperation(operation)
      end
      sizeBuffer

      if writeAttr != 0
        writeHeaderWithPolicy(policy, readAttr, writeAttr, fieldCount, operations.length)
      else
        writeHeader(readAttr, writeAttr, fieldCount, operations.length)
      end
      writeKey(key)

      operations.each do |operation|
        writeOperationForOperation(operation)
      end

      writeOperationForBin(nil, Apik::Operation::READ) if readHeader

      end_cmd
    end

    def setUdf(key, packageName, functionName, args)
      begin_cmd
      fieldCount = estimateKeySize(key)
      argBytes = packValueArray(args)

      fieldCount += estimateUdfSize(packageName, functionName, argBytes)
      sizeBuffer

      writeHeader(0, INFO2_WRITE, fieldCount, 0)
      writeKey(key)
      writeFieldString(packageName, UDF_PACKAGE_NAME)
      writeFieldString(functionName, UDF_FUNCTION)
      writeFieldBytes(argBytes, UDF_ARGLIST)

      end_cmd
    end

    def setBatchExists(batchNamespace)
      # Estimate buffer size
      begin_cmd
      keys = batchNamespace.keys
      byteSize = keys.length * DIGEST_SIZE

      @dataOffset += (batchNamespace ? batchNamespace.namespace.bytesize : 0)  +
        FIELD_HEADER_SIZE + byteSize + FIELD_HEADER_SIZE

      sizeBuffer

      writeHeader(INFO1_READ|INFO1_NOBINDATA, 0, 2, 0)
      writeFieldString(batchNamespace.namespace, Apik::FieldType::NAMESPACE)
      writeFieldHeader(byteSize, Apik::FieldType::DIGEST_RIPE_ARRAY)

      keys.each do |key|
        digest = key.digest
        @dataBuffer.write_binary(digest, @dataOffset)
        @dataOffset += digest.bytesize
      end
      end_cmd
    end

    def setBatchGet(batchNamespace, binNames, readAttr)
      # Estimate buffer size
      begin_cmd
      keys = batchNamespace.keys
      byteSize = keys.length * DIGEST_SIZE

      @dataOffset += batchNamespace.namespace.bytesize +
        FIELD_HEADER_SIZE + byteSize + FIELD_HEADER_SIZE

      if binNames
        binNames.eahc do |binName|
          estimateOperationSizeForBinName(binName)
        end
      end

      sizeBuffer

      operationCount = 0
      if binNames
        operationCount = binNames.length
      end

      writeHeader(readAttr, 0, 2, operationCount)
      writeFieldString(batchNamespace.namespace, Apik::FieldType::NAMESPACE)
      writeFieldHeader(byteSize, Apik::FieldType::DIGEST_RIPE_ARRAY)

      keys.each do |key|
        digest = key.digest
        @dataBuffer.write_binary(digest, @dataOffset)
        @dataOffset += digest.length
      end

      if binNames
        binNames.each do |binName|
          writeOperationForBinName(binName, Apik::Operation::READ)
        end
      end

      end_cmd
    end

    def execute
      iterations = 0

      # set timeout outside the loop
      limit = Time.now + @policy.Timeout

      # Execute command until successful, timed out or maximum iterations have been reached.
      while true
        # too many retries
        iterations += 1
        break if (@policy.MaxRetries > 0) && (iterations > @policy.MaxRetries+1)

        # Sleep before trying again, after the first iteration
        sleep(@policy.SleepBetweenRetries) if iterations > 1 && @policy.SleepBetweenRetries > 0

        # check for command timeout
        break if @policy.Timeout > 0 && Time.now > limit

        begin
          @conn = @node.get_connection(@policy.Timeout)
        rescue Exception => e
          # Socket connection error has occurred. Decrease health and retry.
          @node.decrease_health

          Apik.logger.warn("Node #{@node.to_s}: #{e}")
          next
        end

        # Draw a buffer from buffer pool, and make sure it will be put back
        begin
          @dataBuffer = Buffer.get

          # Set command buffer.
          begin
            writeBuffer
          rescue Exception => e
            # All runtime exceptions are considered fatal. Do not retry.
            # Close socket to flush out possible garbage. Do not put back in pool.
            @conn.close
            raise e
          end

          # Reset timeout in send buffer (destined for server) and socket.
          @dataBuffer.write_int32((@policy.Timeout * 1000).to_i, 22)

          # Send command.
          begin
            @conn.write(@dataBuffer, @dataOffset)
          rescue Exception => e
            # IO errors are considered temporary anomalies. Retry.
            # Close socket to flush out possible garbage. Do not put back in pool.
            @conn.close

            Apik.logger.warn("Node #{@node.to_s}: #{e}")
            # IO error means connection to server @node is unhealthy.
            # Reflect cmd status.
            @node.decrease_health
            next
          end

          # Parse results.
          begin
            parseResult
          rescue Exception => e
            # close the connection
            # cancelling/closing the batch/multi commands will return an error, which will
            # close the connection to throw away its data and signal the server about the
            # situation. We will not put back the connection in the buffer.
            @conn.close
            raise e
          end

          # Reflect healthy status.
          @node.restore_health

          # Put connection back in pool.
          @node.put_connection(@conn)

          # command has completed successfully.  Exit method.
          return
        ensure
          Buffer.put(@dataBuffer)
        end

      end # while

      # execution timeout
      raise Apik::Exceptions::Timeout.new(limit, iterations)
    end

    protected


    def estimateKeySize(key)
      fieldCount = 0

      if key.namespace
        @dataOffset += key.namespace.length + FIELD_HEADER_SIZE
        fieldCount += 1
      end

      if key.setName
        @dataOffset += key.setName.length + FIELD_HEADER_SIZE
        fieldCount += 1
      end

      @dataOffset += key.digest.length + FIELD_HEADER_SIZE
      fieldCount += 1

      return fieldCount
    end

    def estimateUdfSize(packageName, functionName, bytes)
      @dataOffset += packageName.bytesize + FIELD_HEADER_SIZE
      @dataOffset += functionName.bytesize + FIELD_HEADER_SIZE
      @dataOffset += bytes.bytesize + FIELD_HEADER_SIZE
      return 3
    end

    def estimateOperationSizeForBin(bin)
      @dataOffset += bin.name.length + OPERATION_HEADER_SIZE
      @dataOffset += bin.value_object.estimateSize
    end

    def estimateOperationSizeForOperation(operation)
      binLen = 0

      if operation.bin_name
        binLen = operation.bin_name.length
      end

      @dataOffset += binLen + OPERATION_HEADER_SIZE

      if operation.bin_value
        @dataOffset += operation.bin_value.estimateSize
      end
    end

    def estimateOperationSizeForBinName(binName)
      @dataOffset += binName.length + OPERATION_HEADER_SIZE
    end

    def estimateOperationSize
      @dataOffset += OPERATION_HEADER_SIZE
    end

    # Generic header write.
    def writeHeader(readAttr, writeAttr, fieldCount, operationCount)
      # Write all header data except total size which must be written last.
      @dataBuffer.write_byte(MSG_REMAINING_HEADER_SIZE, 8) # Message heade.length.
      @dataBuffer.write_byte(readAttr, 9)
      @dataBuffer.write_byte(writeAttr, 10)

      for i in 11...26
        @dataBuffer.write_byte(0, i)
      end

      # Buffer.Int16ToBytes(fieldCount, @dataBuffer, 26)
      # Buffer.Int16ToBytes(operationCount, @dataBuffer, 28)
      @dataBuffer.write_int16(fieldCount, 26)
      @dataBuffer.write_int16(operationCount, 28)

      @dataOffset = MSG_TOTAL_HEADER_SIZE
    end

    # Header write for write operations.
    def writeHeaderWithPolicy(policy, readAttr, writeAttr, fieldCount, operationCount)
      # Set flags.
      generation = Integer(0)
      infoAttr = Integer(0)

      case policy.RecordExistsAction
      when Apik::RecordExistsAction::UPDATE
      when Apik::RecordExistsAction::UPDATE_ONLY
        infoAttr |= INFO3_UPDATE_ONLY
      when Apik::RecordExistsAction::REPLACE
        infoAttr |= INFO3_CREATE_OR_REPLACE
      when Apik::RecordExistsAction::REPLACE_ONLY
        infoAttr |= INFO3_REPLACE_ONLY
      when Apik::RecordExistsAction::CREATE_ONLY
        writeAttr |= INFO2_CREATE_ONLY
      end

      case policy.GenerationPolicy
      when Apik::GenerationPolicy::NONE
      when Apik::GenerationPolicy::EXPECT_GEN_EQUAL
        generation = policy.Generation
        writeAttr |= INFO2_GENERATION
      when Apik::GenerationPolicy::EXPECT_GEN_GT
        generation = policy.Generation
        writeAttr |= INFO2_GENERATION_GT
      when Apik::GenerationPolicy::DUPLICATE
        generation = policy.Generation
        writeAttr |= INFO2_GENERATION_DUP
      end

      # Write all header data except total size which must be written last.
      @dataBuffer.write_byte(MSG_REMAINING_HEADER_SIZE, 8) # Message heade.length.
      @dataBuffer.write_byte(readAttr, 9)
      @dataBuffer.write_byte(writeAttr, 10)
      @dataBuffer.write_byte(infoAttr, 11)
      @dataBuffer.write_byte(0, 12) # unused
      @dataBuffer.write_byte(0, 13) # clear the result code
      # Buffer.Int32ToBytes(generation, @dataBuffer, 14)
      @dataBuffer.write_int32(generation, 14)
      # Buffer.Int32ToBytes(policy.Expiration, @dataBuffer, 18)
      @dataBuffer.write_int32(policy.Expiration, 18)

      # Initialize timeout. It will be written later.
      @dataBuffer.write_byte(0, 22)
      @dataBuffer.write_byte(0, 23)
      @dataBuffer.write_byte(0, 24)
      @dataBuffer.write_byte(0, 25)


      # Buffer.Int16ToBytes(fieldCount, @dataBuffer, 26)
      @dataBuffer.write_int16(fieldCount, 26)
      # Buffer.Int16ToBytes(operationCount, @dataBuffer, 28)
      @dataBuffer.write_int16(operationCount, 28)

      @dataOffset = MSG_TOTAL_HEADER_SIZE
    end

    def writeKey(key)
      # Write key into buffer.
      if key.namespace
        writeFieldString(key.namespace, Apik::FieldType::NAMESPACE)
      end

      if key.setName
        writeFieldString(key.setName, Apik::FieldType::TABLE)
      end

      writeFieldBytes(key.digest, Apik::FieldType::DIGEST_RIPE)
    end

    def writeOperationForBin(bin, operation)
      nameLength = @dataBuffer.write_binary(bin.name, @dataOffset+OPERATION_HEADER_SIZE)
      valueLength = bin.value_object.write(@dataBuffer, @dataOffset+OPERATION_HEADER_SIZE+nameLength)

      # Buffer.Int32ToBytes(nameLength+valueLength+4, @dataBuffer, @dataOffset)
      @dataBuffer.write_int32(nameLength+valueLength+4, @dataOffset)

      @dataOffset += 4
      @dataBuffer.write_byte(operation, @dataOffset)
      @dataOffset += 1
      @dataBuffer.write_byte(bin.value_object.type, @dataOffset)
      @dataOffset += 1
      @dataBuffer.write_byte(0, @dataOffset)
      @dataOffset += 1
      @dataBuffer.write_byte(nameLength, @dataOffset)
      @dataOffset += 1
      @dataOffset += nameLength + valueLength
    end

    def writeOperationForOperation(operation)
      nameLength = 0
      if operation.bin_name
        nameLength = @dataBuffer.write_binary(operation.bin_name, @dataOffset+OPERATION_HEADER_SIZE)
      end

      valueLength = operation.bin_value.write(@dataBuffer, @dataOffset+OPERATION_HEADER_SIZE+nameLength)

      # Buffer.Int32ToBytes(nameLength+valueLength+4, @dataBuffer, @dataOffset)
      @dataBuffer.write_int32(nameLength+valueLength+4, @dataOffset)

      @dataOffset += 4
      @dataBuffer.write_byte(operation.op_type, @dataOffset)
      @dataOffset += 1
      @dataBuffer.write_byte(operation.bin_value.type, @dataOffset)
      @dataOffset += 1
      @dataBuffer.write_byte(0, @dataOffset)
      @dataOffset += 1
      @dataBuffer.write_byte(nameLength, @dataOffset)
      @dataOffset += 1
      @dataOffset += nameLength + valueLength
    end

    def writeOperationForBinName(name, operation)
      nameLength = @dataBuffer.write_binary(name, @dataOffset+OPERATION_HEADER_SIZE)
      # Buffer.Int32ToBytes(nameLength+4, @dataBuffer, @dataOffset)
      @dataBuffer.write_int32(nameLength+4, @dataOffset)

      @dataOffset += 4
      @dataBuffer.write_byte(operation, @dataOffset)
      @dataOffset += 1
      @dataBuffer.write_byte(0, @dataOffset)
      @dataOffset += 1
      @dataBuffer.write_byte(0, @dataOffset)
      @dataOffset += 1
      @dataBuffer.write_byte(nameLength, @dataOffset)
      @dataOffset += 1
      @dataOffset += nameLength
    end

    def writeOperationForOperationType(operation)
      # Buffer.Int32ToBytes(4), @dataBuffer, @dataOffset
      @dataBuffer.write_int32(4, @dataOffset)
      @dataOffset += 4
      @dataBuffer.write_byte(operation, @dataOffset)
      @dataOffset += 1
      @dataBuffer.write_byte(0, @dataOffset)
      @dataOffset += 1
      @dataBuffer.write_byte(0, @dataOffset)
      @dataOffset += 1
      @dataBuffer.write_byte(0, @dataOffset)
      @dataOffset += 1
    end

    def writeFieldValue(value, ftype)
      offset = @dataOffset + FIELD_HEADER_SIZE
      @dataBuffer.write_byte(value.type, offset)
      offset += 1
      len = value.write(@dataBuffer, offset)
      len += 1
      writeFieldHeader(len, ftype)
      @dataOffset += len
    end

    def writeFieldString(str, ftype)
      len = @dataBuffer.write_binary(str, @dataOffset+FIELD_HEADER_SIZE)
      writeFieldHeader(len, ftype)
      @dataOffset += len
    end

    def writeFieldBytes(bytes, ftype)
      @dataBuffer.write_binary(bytes, @dataOffset+FIELD_HEADER_SIZE)

      writeFieldHeader(bytes.bytesize, ftype)
      @dataOffset += bytes.bytesize
    end

    def writeFieldHeader(size, ftype)
      # Buffer.Int32ToBytes(size+1), @dataBuffer, @dataOffset
      @dataBuffer.write_int32(size+1, @dataOffset)
      @dataOffset += 4
      @dataBuffer.write_byte(ftype, @dataOffset)
      @dataOffset += 1
    end

    def begin_cmd
      @dataOffset = MSG_TOTAL_HEADER_SIZE
    end

    def sizeBuffer
      sizeBufferSz(@dataOffset)
    end

    def sizeBufferSz(size)
      # Corrupted data streams can result in a hug.length.
      # Do a sanity check here.
      if size > Buffer::MAX_BUFFER_SIZE
        raise Apik::Exceptions::Aerospike.new(PARSE_ERROR, "Invalid size for buffer: #{size}")
      end

      @dataBuffer.resize(size)
    end

    def end_cmd
      size = (@dataOffset-8) | Integer(CL_MSG_VERSION << 56) | Integer(AS_MSG_TYPE << 48)
      @dataBuffer.write_int64(size, 0)
    end

  end # class

end # module
