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
require 'apik/command/write_command'

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
    # def get_header(policy, key)
    #   policy ||= Policy.new
    #   command = ReadHeaderCommand.new(@cluster, policy, key)
    #   command.execute()
    #   command.GetRecord
    # end

  end # class

end #module
