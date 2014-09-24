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

require 'digest'
require 'optionable'

require 'apik/operation'

require 'apik/cluster/cluster'
require 'apik/policy/client_policy'

require 'apik/command/read_command'
require 'apik/command/read_header_command'
require 'apik/command/write_command'
require 'apik/command/delete_command'
require 'apik/command/exists_command'
require 'apik/command/touch_command'
require 'apik/command/operate_command'

require 'apik/command/batch_command_get'
require 'apik/command/batch_command_exists'
require 'apik/command/batch_node'
require 'apik/command/batch_item'

module Apik

  class Client
    include Optionable

    def initialize(policy, host, port)
      policy ||= ClientPolicy.new

      @cluster = Cluster.new(policy, Host.new(host, port))

      self
    end

    #  Close all client connections to database server nodes.
    def close
      @cluster.close
    end

    #  Determine if we are ready to talk to the database server cluster.
    def connected?
      @cluster.connected?
    end

    #  Return array of active server nodes in the cluster.
    def get_nodes
      @cluster.get_nodes
    end

    #  Return list of active server node names in the cluster.
    def get_node_names
      nodes = @cluster.get_nodes
      names = []

      nodes.each do |node|
        names << node.get_name
      end

      names
    end

    #-------------------------------------------------------
    # Write Record Operations
    #-------------------------------------------------------

    #  Write record bin(s).
    #  The policy specifies the transaction timeout, record expiration and how the transaction is
    #  handled when the record already exists.
    # def put(policy *WritePolicy, key *Key, bins BinMap) error {
    #   PutBins(policy, key, binMapToBins(bins)...)
    # }

    #  Write record bin(s).
    #  The policy specifies the transaction timeout, record expiration and how the transaction is
    #  handled when the record already exists.
    def put_bins(policy, key, *bins)
      policy ||= WritePolicy.new(0, 0)
      command = WriteCommand.new(@cluster, policy, key, bins, Apik::Operation::WRITE)
      command.execute()
    end

    #-------------------------------------------------------
    # Operations string
    #-------------------------------------------------------

    #  Append bin values string to existing record bin values.
    #  The policy specifies the transaction timeout, record expiration and how the transaction is
    #  handled when the record already exists.
    #  This call only works for string values.
    # def Append(policy *WritePolicy, key *Key, bins BinMap) error {
    #   return clnt.AppendBins(policy, key, binMapToBins(bins)...)
    # }

    def append_bins(policy, key, bins)
      policy ||= WritePolicy.new(0, 0)
      command = WriteCommand.new(@cluster, policy, key, bins, Apik::Operation::APPEND)
      command.execute()
    end

    #  Prepend bin values string to existing record bin values.
    #  The policy specifies the transaction timeout, record expiration and how the transaction is
    #  handled when the record already exists.
    #  This call works only for string values.
    # def Prepend(policy *WritePolicy, key *Key, bins BinMap) error {
    #   return clnt.PrependBins(policy, key, binMapToBins(bins)...)
    # }

    def prepend_bins(policy, key, bins)
      policy ||= WritePolicy.new(0, 0)
      command = WriteCommand.new(@cluster, policy, key, bins, Apik::Operation::PREPEND)
      command.execute()
    end

    #-------------------------------------------------------
    # Arithmetic Operations
    #-------------------------------------------------------

    #  Add integer bin values to existing record bin values.
    #  The policy specifies the transaction timeout, record expiration and how the transaction is
    #  handled when the record already exists.
    #  This call only works for integer values.
    # def Add(policy *WritePolicy, key *Key, bins BinMap) error {
    #   return clnt.AddBins(policy, key, binMapToBins(bins)...)
    # }

    def add_bins(policy, key, bins)
      policy ||= WritePolicy.new(0, 0)
      command = WriteCommand.new(@cluster, policy, key, bins, Apik::Operation::ADD)
      command.execute()
    end

    #-------------------------------------------------------
    # Delete Operations
    #-------------------------------------------------------

    #  Delete record for specified key.
    #  The policy specifies the transaction timeout.
    def delete(policy, key)
      policy ||= WritePolicy.new(0, 0)
      command = DeleteCommand.new(@cluster, policy, key)
      command.execute()
      command.existed
    end

    #-------------------------------------------------------
    # Touch Operations
    #-------------------------------------------------------

    #  Create record if it does not already exist.  If the record exists, the record's
    #  time to expiration will be reset to the policy's expiration.
    def touch(policy, key)
      policy ||= WritePolicy.new(0, 0)
      command = TouchCommand.new(@cluster, policy, key)
      command.execute()
    end

    #-------------------------------------------------------
    # Existence-Check Operations
    #-------------------------------------------------------

    #  Determine if a record key exists.
    #  The policy can be used to specify timeouts.
    def exists(policy, key)
      policy ||= Policy.new
      command = ExistsCommand.new(@cluster, policy, key)
      command.execute()
      command.exists
    end

    #  Check if multiple record keys exist in one batch call.
    #  The returned array bool is in positional order with the original key array order.
    #  The policy can be used to specify timeouts.
    def batch_exists(policy, keys)
      policy ||= Policy.new

      # same array can be used without sychronization;
      # when a key exists, the corresponding index will be marked true
      existsArray = Array.new(keys.length)

      keyMap = BatchItem.generate_map(keys)

      cmdGen = Proc.new do |node, bns|
        BatchCommandExists.new(node, bns, policy, keyMap, existsArray)
      end

      batch_execute(keys, &cmdGen)
      existsArray
    end

    #-------------------------------------------------------
    # Read Record Operations
    #-------------------------------------------------------

    #  Read record header and bins for specified key.
    #  The policy can be used to specify timeouts.
    def get(policy, key, *binNames)
      policy ||= Policy.new

      command = ReadCommand.new(@cluster, policy, key, binNames)
      command.execute()
      command.record
    end

    #  Read record generation and expiration only for specified key.  Bins are not read.
    #  The policy can be used to specify timeouts.
    def get_header(policy, key)
      policy ||= Policy.new
      command = ReadHeaderCommand.new(@cluster, policy, key)
      command.execute()
      command.record
    end

    #-------------------------------------------------------
    # Batch Read Operations
    #-------------------------------------------------------

    #  Read multiple record headers and bins for specified keys in one batch call.
    #  The returned records are in positional order with the original key array order.
    #  If a key is not found, the positional record will be nil.
    #  The policy can be used to specify timeouts.
    def batch_get(policy, keys, *binNames)
      policy ||= Policy.new

      # wait until all migrations are finished
      # TODO: implement
      # @cluster.WaitUntillMigrationIsFinished(policy.timeout())

      # same array can be used without sychronization;
      # when a key exists, the corresponding index will be set to record
      records = Array.new(keys.length)

      keyMap = BatchItem.generate_map(keys)
      binSet = {}
      binNames.each do |bn|
        binSet[bn] = {}
      end


      cmdGen = Proc.new do |node, bns|
        BatchCommandGet.new(node, bns, policy, keyMap, nil, records, INFO1_READ|INFO1_NOBINDATA)
      end

      batch_execute(keys, &cmdGen)
      records
    end

    #  Read multiple record header data for specified keys in one batch call.
    #  The returned records are in positional order with the original key array order.
    #  If a key is not found, the positional record will be nil.
    #  The policy can be used to specify timeouts.
    def batch_get_header(policy, keys)
      policy ||= Policy.new

      # wait until all migrations are finished
      # TODO: Fix this and implement
      # @cluster.WaitUntillMigrationIsFinished(policy.timeout())

      # same array can be used without sychronization;
      # when a key exists, the corresponding index will be set to record
      records = Array.new(keys.length)

      keyMap = BatchItem.generate_map(keys)
      binSet = {}
      binNames.each do |bn|
        binSet[bn] = {}
      end

      command = BatchCommandGet.new(node, bns, policy, keyMap, binSet, records, INFO1_READ | INFO_NOBINDATA)
      command.execute
    end

    #-------------------------------------------------------
    # Generic Database Operations
    #-------------------------------------------------------

    #  Perform multiple read/write operations on a single key in one batch call.
    #  An example would be to add an integer value to an existing record and then
    #  read the result, all in one database call.
    #
    #  Write operations are always performed first, regardless of operation order
    #  relative to read operations.
    def operate(policy, key, *operations)
      policy ||= WritePolicy.new(0, 0)

      command = OperateCommand.new(@cluster, policy, key, operations)
      command.execute()
      command.record
    end

    private

    def batch_execute(keys, &cmdGen)
      batchNodes = BatchNode.generate_list(@cluster, keys)
      threads = []

      # Use a thread per namespace per node
      batchNodes.each do |batchNode|
        # copy to avoid race condition
        bn = batchNode
        bn.batch_namespaces.each do |bns|
          threads << Thread.new do
            command = cmdGen.call(bn.node, bns)
            command.execute
          end
        end
      end

      threads.each { |thr| thr.join }
    end

  end # class

end #module
