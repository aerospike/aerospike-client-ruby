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

      SUPPORTED_TLS_PARAMS = %i[ca_file ca_path min_version max_version].freeze
      DEFAULT_TLS_PARAMS = {
        min_version: :TLS1_2
      }.freeze

      class << self
        def connect(host, port, timeout, tls_name, tls_options)
          Aerospike.logger.debug("Connecting to #{host}:#{tls_name}:#{port} using TLS options #{tls_options}")
          tcp_sock = TCP.connect(host, port, timeout)
          ctx = build_ssl_context(tls_options)
          new(tcp_sock, ctx).tap do |ssl_sock|
            ssl_sock.hostname = tls_name
            ssl_sock.connect
            ssl_sock.post_connection_check(tls_name)
          end
        end

        def build_ssl_context(tls_options)
          tls_options[:context] || create_context(tls_options)
        end

        def create_context(tls_options)
          OpenSSL::SSL::SSLContext.new.tap do |ctx|
            if tls_options[:cert_file] && tls_options[:pkey_file]
              cert = OpenSSL::X509::Certificate.new(File.read(tls_options[:cert_file]))
              pkey = OpenSSL::PKey.read(File.read(tls_options[:pkey_file]), tls_options[:pkey_pass])
              if ctx.respond_to?(:add_certificate)
                ctx.add_certificate(cert, pkey)
              else
                ctx.cert = cert
                ctx.key = pkey
              end
            end

            params = DEFAULT_TLS_PARAMS.merge(filter_params(tls_options))
            ctx.set_params(params) unless params.empty?
          end
        end

        def filter_params(params)
          params.select { |key| SUPPORTED_TLS_PARAMS.include?(key) }
        end
      end
    end
  end
end
