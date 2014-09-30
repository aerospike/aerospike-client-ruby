# encoding: utf-8
# Copyright 2014 Aerospike, Inc.
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

module Aerospike

  protected

  class NodeValidator

    attr_reader :host, :aliases, :name, :use_new_info

    def initialize(host, timeout)
      @use_new_info = true
      @host = host

      set_aliases(host)
      set_address(timeout)

      self
    end

    def set_aliases(host)
      addresses = Resolv.getaddresses(host.name)
      aliases = []
      addresses.each do |addr|
        aliases << Host.new(addr, host.port)
      end

      @aliases = aliases

      Aerospike.logger.debug("Node Validator has #{aliases.length} nodes.")
    end

    def set_address(timeout)
      @aliases.each do |aliass|
        begin
          conn = Connection.new(aliass.name, aliass.port, 1)
          conn.timeout = timeout

          info_map= Info.request(conn, 'node', 'build')
          if node_name = info_map['node']
            @name = node_name

            # Check new info protocol support for >= 2.6.6 build
            if build_version = info_map['build']
              v1, v2, v3 = parse_version_string(build_version)
              @use_new_info = v1.to_i > 2 || (v1.to_i == 2 && (v2.to_i > 6 || (v2.to_i == 6 && v3.to_i >= 6)))
            end
          end
        ensure
          conn.close if conn
        end

      end
    end

    protected

    # parses a version string
    @@version_regexp = /(?<v1>\d+)\.(?<v2>\d+)\.(?<v3>\d+).*/

    def parse_version_string(version)
      if v = @@version_regexp.match(version)
        return v['v1'], v['v2'], v['v3']
      end

      raise Aerospike::Exceptions::Parse.new("Invalid build version string in Info: #{version}")
    end

  end # class

end #module
