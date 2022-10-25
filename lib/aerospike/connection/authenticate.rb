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
  module Connection # :nodoc:
    module Authenticate
      class << self
        def call(conn, user, hashed_pass)
          command = LoginCommand.new
          command.authenticate(conn, user, hashed_pass)
          true
        rescue ::Aerospike::Exceptions::Aerospike
          conn.close if conn
          raise ::Aerospike::Exceptions::InvalidCredentials
        end
      end
    end
    module AuthenticateNew
      class << self
        INVALID_SESSION_ERR = [ResultCode::INVALID_CREDENTIAL,
          ResultCode::EXPIRED_SESSION]

        def call(conn, cluster)
          command = LoginCommand.new
          if cluster.session_valid?
            begin
              command.authenticate_via_token(conn, cluster)
            rescue => ae
              # always reset session info on errors to be on the safe side
              cluster.reset_session_info
              if ae.is_a?(Exceptions::Aerospike)
                if INVALID_SESSION_ERR.include?(ae.result_code)
                  command.authenticate_new(conn, cluster)
                  return true
                end
              end
              raise ae
            end
          else
            command.authenticate_new(conn, cluster)
          end

          true
        rescue ::Aerospike::Exceptions::Aerospike
          conn.close if conn
          raise ::Aerospike::Exceptions::InvalidCredentials
        end
      end
    end
  end
end

