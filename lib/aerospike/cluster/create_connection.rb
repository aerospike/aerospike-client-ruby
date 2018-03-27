# frozen_string_literal: true

# Copyright 2018 Aerospike, Inc.
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
  class Cluster
    # Create connection based on cluster config and authenticate if needed
    module CreateConnection
      class << self
        def call(cluster, host)
          ::Aerospike::Connection::Create.(
            host.name,
            host.port,
            tls_name: host.tls_name,
            timeout: cluster.connection_timeout,
            ssl_options: cluster.ssl_options
          ).tap do |conn|
            if cluster.credentials_given?
              # Authenticate will raise and close connection if invalid credentials
              Connection::Authenticate.(conn, cluster.user, cluster.password)
            end
          end
        end
      end
    end
  end
end
