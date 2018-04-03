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
  module Socket
    class SSL < ::OpenSSL::SSL::SSLSocket
      include Base

      SUPPORTED_SSL_PARAMS = %i[ca_file ca_path min_version max_version].freeze
      DEFAULT_SSL_PARAMS = {
        min_version: :TLS1_2
      }.freeze

      class << self
        def connect(host, port, timeout, tls_name, ssl_options)
          Aerospike.logger.debug("Connecting to #{host}:#{tls_name}:#{port} using SSL options #{ssl_options}")
          tcp_sock = TCP.connect(host, port, timeout)
          ctx = build_ssl_context(ssl_options)
          new(tcp_sock, ctx).tap do |ssl_sock|
            ssl_sock.hostname = tls_name
            ssl_sock.connect
            ssl_sock.post_connection_check(tls_name)
          end
        end

        def build_ssl_context(ssl_options)
          ssl_options[:context] || create_context(ssl_options)
        end

        def create_context(ssl_options)
          OpenSSL::SSL::SSLContext.new.tap do |ctx|
            if ssl_options[:cert_file] && ssl_options[:pkey_file]
              cert = OpenSSL::X509::Certificate.new(File.read(ssl_options[:cert_file]))
              pkey = OpenSSL::PKey.read(File.read(ssl_options[:pkey_file]), ssl_options[:pkey_pass])
              if ctx.respond_to?(:add_certificate)
                ctx.add_certificate(cert, pkey)
              else
                ctx.cert = cert
                ctx.key = pkey
              end
            end

            params = DEFAULT_SSL_PARAMS.merge(filter_params(ssl_options))
            ctx.set_params(params) unless params.empty?
          end
        end

        def filter_params(params)
          params.select { |key| SUPPORTED_SSL_PARAMS.include?(key) }
        end
      end
    end
  end
end
