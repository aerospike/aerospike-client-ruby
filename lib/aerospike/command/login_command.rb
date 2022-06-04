# encoding: utf-8
# Copyright 2014-2020 Aerospike, Inc.
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

require 'aerospike/command/admin_command'

module Aerospike

  private

  attr_reader :session_token, :session_expiration

  class LoginCommand < AdminCommand #:nodoc:

    def login(conn, policy)
        hashed_pass = LoginCommand.hash_password(policy.password)
        authenticate(conn, policy, hashed_pass)
    end

    def authenticate(conn, user, hashed_pass)
      write_header(LOGIN, 2)
      write_field_str(USER, policy.user)
      write_field_bytes(CREDENTIAL, hashed_pass)

      parse_tokens(conn)
    end

    def authenticate_new(conn, cluster)
      policy = cluster.client_policy
      case policy.auth_mode
      when Aerospike::AuthMode::EXTERNAL
        write_header(LOGIN, 3)
        write_field_str(USER, policy.user)
        write_field_bytes(CREDENTIAL, cluster.password)
        write_field_str(CLEAR_PASSWORD, policy.password)
      when Aerospike::AuthMode::INTERNAL
        write_header(LOGIN, 2)
        write_field_str(USER, policy.user)
        write_field_bytes(CREDENTIAL, cluster.password)
      when Aerospike::AuthMode::PKI
        write_header(LOGIN, 0)
      else
        raise Exceptions::Aerospike.new(Aerospike::ResultCode::COMMAND_REJECTED, "Invalid client_policy#auth_mode.")
      end

      parse_tokens(conn)
      cluster.session_token = @session_token
      cluster.session_expiration = @session_expiration
    end

    def parse_tokens(conn)
      begin
        write_size
        conn.write(@data_buffer, @data_offset)
        conn.read(@data_buffer, HEADER_SIZE)

        result = @data_buffer.read(RESULT_CODE)

        if result != 0 
          return if result == Aerospike::ResultCode::SECURITY_NOT_ENABLED
          raise Exceptions::Aerospike.new(result, "Authentication failed")
        end

        # read the rest of the buffer
        size = @data_buffer.read_int64(0)
        receive_size = (size & 0xFFFFFFFFFFFF) - HEADER_REMAINING
        field_count = @data_buffer.read(11) & 0xFF

        if receive_size <= 0 || receive_size > @data_buffer.size || field_count <= 0
          raise Exceptions::Aerospike.new(result, "Node failed to retrieve session token")
        end

        if @data_buffer.size < receive_size
          @data_buffer.resize(receive_size)
        end

        conn.read(@data_buffer, receive_size)

        @data_offset = 0
        for i in 0...field_count
          mlen = @data_buffer.read_int32(@data_offset)
          @data_offset += 4
          id = @data_buffer.read(@data_offset)
          @data_offset += 1
          mlen -= 1

          case id
          when SESSION_TOKEN
            # copy the contents of the buffer into a new byte slice
            @session_token = @data_buffer.read(@data_offset, mlen)

          when SESSION_TTL
            # Subtract 60 seconds from TTL so client session expires before server session.
            seconds = @data_buffer.read_int32(@data_offset) - 60

            if seconds > 0
              @session_expiration = Time.now + (seconds/86400)
            else
              Aerospike.logger.warn("Invalid session TTL: #{seconds}")
              raise Exceptions::Aerospike.new(result, "Node failed to retrieve session token")
            end
          end

          @data_offset += mlen
        end

        if !@session_token
          raise Exceptions::Aerospike.new(result, "Node failed to retrieve session token")
        end
      ensure
        Buffer.put(@data_buffer)
      end
    end

    def authenticate_via_token(conn, cluster)
      policy = cluster.client_policy
      if policy.auth_mode != Aerospike::AuthMode::PKI
        write_header(AUTHENTICATE, 2)
        write_field_str(USER, policy.user)
      else
        write_header(AUTHENTICATE, 1)
      end

      write_field_bytes(SESSION_TOKEN, cluster.session_token) if cluster.session_token
      write_size

      conn.write(@data_buffer, @data_offset)
      conn.read(@data_buffer, HEADER_SIZE)

      result = @data_buffer.read(RESULT_CODE)
      size = @data_buffer.read_int64(0)
      receive_size = (size & 0xFFFFFFFFFFFF) - HEADER_REMAINING
      conn.read(@data_buffer, receive_size)

      if result != 0 
        return if result == Aerospike::ResultCode::SECURITY_NOT_ENABLED
        raise Exceptions::Aerospike.new(result, "Authentication failed")
      end

      nil
    end

    SALT = '$2a$10$7EqJtq98hPqEX7fNZaFWoO'
    def self.hash_password(password)
      # Hashing the password with the cost of 10, with a static salt
      return BCrypt::Engine.hash_secret(password, SALT, :cost => 10)
    end
  end
end

