# frozen_string_literal: true

# Copyright 2014-2019 Aerospike, Inc.
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
  class NodeValidator # :nodoc:

    attr_reader :host, :aliases, :name, :features, :cluster_name, :tls_options, :conn

    def initialize(cluster, host, timeout, cluster_name, tls_options = {})
      @cluster = cluster
      @features = Set.new
      @host = host
      @cluster_name = cluster_name
      @tls_options = tls_options

      @aliases = []

      resolve(host.name).each do |address|
        @aliases += get_hosts(address)
      end
    end

    private

    def get_hosts(address)
      aliases = [get_alias(address, host.port)]

      begin
        conn = Cluster::CreateConnection.(@cluster, Host.new(address, host.port, host.tls_name))

        commands = %w[node features]
        commands << address_command unless is_loopback?(address)

        info_map = Info.request(conn, *commands)

        if node_name = info_map['node']
          @name = node_name

          # Set features
          if features = info_map['features']
            @features = features.split(';').to_set
          end
        end

        unless is_loopback?(address)
          aliases = info_map[address_command].split(',').map { |addr| get_alias(*addr.split(':')) }
        end
      ensure
        conn.close if conn
      end

      aliases.map { |al| Host.new(al[:address], al[:port], host.tls_name) }
    end

    def get_alias(address, port)
      { address: address, port: port }
    end

    def resolve(hostname)
      if is_ip?(hostname)
        # Don't try to resolve IP addresses.
        # May fail in different OS or network setups
        [hostname]
      else
        Resolv.getaddresses(hostname)
      end
    end

    def address_command
      @address_command ||= @cluster.tls_enabled? ? 'service-tls-std': 'service-clear-std'
    end

    def is_loopback?(address)
      info = Addrinfo.ip(address)
      info.ipv4_loopback? || info.ipv6_loopback?
    end

    def is_ip?(hostname)
      !!((hostname =~ Resolv::IPv4::Regex) || (hostname =~ Resolv::IPv6::Regex))
    end

  end # class
end # module
