# frozen_string_literal: true

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
  class NodeValidator # :nodoc:
    VERSION_REGEXP = /(?<v1>\d+)\.(?<v2>\d+)\.(?<v3>\d+).*/.freeze

    attr_reader :host, :aliases, :name, :use_new_info, :features, :cluster_name, :tls_options, :conn

    def initialize(cluster, host, timeout, cluster_name, tls_options = {})
      @cluster = cluster
      @use_new_info = true
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

        commands = %w[node build features]
        commands << address_command unless is_loopback?(address)

        info_map = Info.request(conn, *commands)

        if node_name = info_map['node']
          @name = node_name

          # Set features
          if features = info_map['features']
            @features = features.split(';').to_set
          end

          # Check new info protocol support for >= 2.6.6 build
          if build_version = info_map['build']
            v1, v2, v3 = parse_version_string(build_version)
            @use_new_info = v1.to_i > 2 || (v1.to_i == 2 && (v2.to_i > 6 || (v2.to_i == 6 && v3.to_i >= 6)))
          end
        end

        unless is_loopback?(address)
          aliases = info_map[address_command].split(',').map { |address| get_alias(*address.split(':')) }
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

    def parse_version_string(version)
      if v = VERSION_REGEXP.match(version)
        return v['v1'], v['v2'], v['v3']
      end

      raise Aerospike::Exceptions::Parse.new("Invalid build version string in Info: #{version}")
    end
  end # class
end # module
