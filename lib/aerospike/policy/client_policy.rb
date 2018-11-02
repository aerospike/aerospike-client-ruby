# encoding: utf-8

# Copyright 2014-2018 Aerospike, Inc.
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

    attr_accessor :user, :password
    attr_accessor :timeout, :connection_queue_size, :fail_if_not_connected, :tend_interval
    attr_accessor :cluster_name
    attr_accessor :tls
    attr_accessor :policies

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

      # user name
      @user = opt[:user]

      # password
      @password = opt[:password]

      # Cluster Name
      @cluster_name = opt[:cluster_name]

      @tls = opt[:tls] || opt[:ssl_options]

      # Default Policies
      @policies = opt.fetch(:policies) { Hash.new }
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
