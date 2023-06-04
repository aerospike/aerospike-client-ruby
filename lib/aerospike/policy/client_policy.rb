# encoding: utf-8

# Copyright 2014-2020 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

module Aerospike

  # Container object for client policy command.
  class ClientPolicy

    attr_accessor :user, :password, :auth_mode
    attr_accessor :timeout, :connection_queue_size, :fail_if_not_connected, :tend_interval, :max_connections_per_node, :min_connections_per_node
    attr_accessor :cluster_name
    attr_accessor :tls
    attr_accessor :policies
    attr_accessor :rack_aware, :rack_id

    def initialize(opt={})
      # Initial host connection timeout in seconds. The timeout when opening a connection
      # to the server host for the first time.
      @timeout = opt[:timeout] || 1.0 # 1 second

      # Size of the Connection Queue cache.
      @connection_queue_size = opt[:connection_queue_size] || 64

      # Throw exception if host connection fails during add_host.
      @fail_if_not_connected = opt.has_key?(:fail_if_not_connected) ? opt[:fail_if_not_connected] : true

      # Tend interval in milliseconds; determines the interval at
      # which the client checks for cluster state changes. Minimum interval is 10ms.
      self.tend_interval = opt[:tend_interval] || 1000 # 1 second

      # Authentication mode
      @auth_mode = opt[:auth_mode] || AuthMode::INTERNAL

      # user name
      @user = opt[:user]

      # password
      @password = opt[:password]

      # Cluster Name
      @cluster_name = opt[:cluster_name]

      @tls = opt[:tls] || opt[:ssl_options]

      # Default Policies
      @policies = opt.fetch(:policies) { Hash.new }

      # Track server rack data.  This field is useful when directing read commands to the server node
      # that contains the key and exists on the same rack as the client.  This serves to lower cloud
      # provider costs when nodes are distributed across different racks/data centers.
      #
      # ClientPolicy#rack_id, Replica#PREFER_RACK and server rack
      # configuration must also be set to enable this functionality.
      @rack_aware = opt[:rack_aware] || false

      # Rack where this client instance resides.
      #
      # ClientPolicy#rack_aware, Replica#PREFER_RACK and server rack
      # configuration must also be set to enable this functionality.
      @rack_id = opt[:rack_id] || 0

      # Maximum number of synchronous connections allowed per server node.  Transactions will go
      # through retry logic and potentially fail with "ResultCode.NO_MORE_CONNECTIONS" if the maximum
      # number of connections would be exceeded.
      # The number of connections used per node depends on concurrent commands in progress
      # plus sub-commands used for parallel multi-node commands (batch, scan, and query).
      # One connection will be used for each command.
      # Default: 100
      @max_connections_per_node = opt[:max_connections_per_node] || 100

      # MinConnectionsPerNode specifies the minimum number of synchronous connections allowed per server node.
      # Preallocate min connections on client node creation.
      # The client will periodically allocate new connections if count falls below min connections.
      #
      # Server proto-fd-idle-ms may also need to be increased substantially if min connections are defined.
      # The proto-fd-idle-ms default directs the server to close connections that are idle for 60 seconds
      # which can defeat the purpose of keeping connections in reserve for a future burst of activity.
      #
      # Default: 0
      @min_connections_per_node = opt[:min_connections_per_node] || 0
    end

    def requires_authentication
      (@user && @user != '') || (@password && @password != '')
    end

    def tend_interval=(interval)
      if interval < 10
        Aerospike.logger.warn("Minimum tend interval is 10 milliseconds (client policy: #{interval}).")
        interval = 10
      end
      @tend_interval = interval
    end

  end # class

end # module
