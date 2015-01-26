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

  private
	# Commands
	AUTHENTICATE    = 0
	CREATE_USER     = 1
	DROP_USER       = 2
	SET_PASSWORD    = 3
	CHANGE_PASSWORD = 4
	GRANT_ROLES     = 5
	REVOKE_ROLES    = 6
	REPLACE_ROLES   = 7
	#CREATE_ROLE = 8
	QUERY_USERS = 9
	#QUERY_ROLES =  10

	# Field IDs
	USER         = 0
	PASSWORD     = 1
	OLD_PASSWORD = 2
	CREDENTIAL   = 3
	ROLES        = 10
	#PRIVILEGES =  11

	# Misc
	MSG_VERSION  = 0
	MSG_TYPE     = 2

	HEADER_SIZE       = 24
	HEADER_REMAINING  = 16
	RESULT_CODE       = 9
	QUERY_END         = 50

	class AdminCommand #:nodoc:

		def initialize
			@data_buffer = Buffer.get
			@data_offset =  8
		end

		def authenticate(conn, user, password)
			begin
				set_authenticate(user, password)
				conn.write(@data_buffer, @data_offset)
				conn.read(@data_buffer, HEADER_SIZE)

				result = @data_buffer.read(RESULT_CODE)
				raise Exceptions::Aerospike.new(result, "Authentication failed") if result != 0
			ensure
				Buffer.put(@data_buffer)
			end
		end

		def set_authenticate(user, password)
			write_header(AUTHENTICATE, 2)
			write_field_str(USER, user)
			write_field_bytes(CREDENTIAL, password)
			write_size

			return @data_offset
		end

		def create_user(cluster, policy, user, password, roles)
			write_header(CREATE_USER, 3)
			write_field_str(USER, user)
			write_field_bytes(PASSWORD, password)
			write_roles(roles)
			execute_command(cluster, policy)
		end

		def drop_user(cluster, policy, user)
			write_header(DROP_USER, 1)
			write_field_str(USER, user)
			execute_command(cluster, policy)
		end

		def set_password(cluster, policy, user, password)
			write_header(SET_PASSWORD, 2)
			write_field_str(USER, user)
			write_field_bytes(PASSWORD, password)
			execute_command(cluster, policy)
		end

		def change_password(cluster, policy, user, password)
			write_header(CHANGE_PASSWORD, 3)
			write_field_str(USER, user)
			write_field_bytes(OLD_PASSWORD, cluster.password)
			write_field_bytes(PASSWORD, password)
			execute_command(cluster, policy)
		end

		def grant_roles(cluster, policy, user, roles)
			write_header(GRANT_ROLES, 2)
			write_field_str(USER, user)
			write_roles(roles)
			execute_command(cluster, policy)
		end

		def revoke_roles(cluster, policy, user, roles)
			write_header(REVOKE_ROLES, 2)
			write_field_str(USER, user)
			write_roles(roles)
			execute_command(cluster, policy)
		end

		def replace_roles(cluster, policy, user, roles)
			write_header(REPLACE_ROLES, 2)
			write_field_str(USER, user)
			write_roles(roles)
			execute_command(cluster, policy)
		end

		def query_user(cluster, policy, user)
			# TODO: Remove the workaround in the future
			sleep(0.010)

			list = []
			begin
				write_header(QUERY_USERS, 1)
				write_field_str(USER, user)
				list = read_users(cluster, policy)
				return (list.is_a?(Array) && list.length > 0 ? list.first : nil)
			ensure
				Buffer.put(@data_buffer)
			end
		end

		def query_users(cluster, policy)
			# TODO: Remove the workaround in the future
			sleep(0.010)
			begin
				write_header(QUERY_USERS, 0)
				return read_users(cluster, policy)
			ensure
				Buffer.put(@data_buffer)
			end
		end

		def write_roles(roles)
			offset = @data_offset + FIELD_HEADER_SIZE
			@data_buffer.write_byte(roles.length.ord, offset)
			offset += 1

			roles.each do |role|
				len = @data_buffer.write_binary(role, offset+1)
				@data_buffer.write_byte(len, offset)
				offset += len + 1
			end

			size = offset - @data_offset - FIELD_HEADER_SIZE
			write_field_header(ROLES, size)
			@data_offset = offset
		end

		def write_size
			# Write total size of message which is the current offset.
			size = Integer(@data_offset-8) | Integer(MSG_VERSION << 56) | Integer(MSG_TYPE << 48)
			@data_buffer.write_int64(size, 0)
		end

		def write_header(command, field_count)
			# Authenticate header is almost all zeros
			i = @data_offset
			while i < @data_offset+16
				@data_buffer.write_byte(0, i)
				i = i.succ
			end
			@data_buffer.write_byte(command, @data_offset+2)
			@data_buffer.write_byte(field_count, @data_offset+3)
			@data_offset += 16
		end

		def write_field_str(id, str)
			len = @data_buffer.write_binary(str, @data_offset+FIELD_HEADER_SIZE)
			write_field_header(id, len)
			@data_offset += len
		end

		def write_field_bytes(id, bytes)
			@data_buffer.write_binary(bytes, @data_offset+FIELD_HEADER_SIZE)
			write_field_header(id, bytes.bytesize)
			@data_offset += bytes.bytesize
		end

		def write_field_header(id, size)
			@data_buffer.write_int32(size+1, @data_offset)
			@data_offset += 4
			@data_buffer.write_byte(id, @data_offset)
			@data_offset += 1
		end

		def execute_command(cluster, policy)
			# TODO: Remove the workaround in the future
			sleep(0.010)

			write_size
			node = cluster.random_node

			timeout = 1
			timeout = policy.timeout if policy && policy.timeout > 0

			conn = node.get_connection(timeout)

			begin
				conn.write(@data_buffer, @data_offset)
				conn.read(@data_buffer, HEADER_SIZE)
				node.put_connection(conn)
			rescue => e
				conn.close if conn
				raise e
			end

			result = @data_buffer.read(RESULT_CODE)
			raise Exceptions::Aerospike.new(result) if result != 0
				
			Buffer.put(@data_buffer)
		end

		def read_users(cluster, policy)
			write_size
			node = cluster.random_node
			
			timeout = 1
			timeout = policy.timeout if policy != nil && policy.timeout > 0

			status = -1
			list = []
			begin							
				conn = node.get_connection(timeout)
				conn.write(@data_buffer, @data_offset)
				status, list = read_user_blocks(conn)
				node.put_connection(conn)
			rescue => e
				conn.close if conn
				raise e
			end

			raise Exceptions::Aerospike.new(result) if status > 0

			return list
		end

		def read_user_blocks(conn)
			rlist = []
			status = 0
			begin
				while status == 0
						conn.read(@data_buffer, 8)
						size = @data_buffer.read_int64(0)
						receive_size = (size & 0xFFFFFFFFFFFF)

						if receive_size > 0
							@data_buffer.resize(receive_size) if receive_size > @data_buffer.size
							
							conn.read(@data_buffer, receive_size)
							status, list = parse_users(receive_size)
							rlist.concat(list.to_a)
						else
							break
						end
				end
				return status, rlist
			rescue => e
				return -1, []
			end
		end

		def parse_users(receive_size)
			@data_offset = 0
			list = []

			while @data_offset < receive_size
				result_code = @data_buffer.read(@data_offset+1)

				if result_code != 0
					return (result_code == QUERY_END ? -1 : result_code)
				end

				userRoles = UserRoles.new
				field_count = @data_buffer.read(@data_offset+3)
				@data_offset += HEADER_REMAINING

				i = 0
				while i  < field_count
					len = @data_buffer.read_int32(@data_offset)
					@data_offset += 4
					id = @data_buffer.read(@data_offset)
					@data_offset += 1
					len -= 1

					case id
						when USER
							userRoles.user = @data_buffer.read(@data_offset, len)
							@data_offset += len
						when ROLES
							parse_roles(userRoles)
						else
							@data_offset += len
					end

					i = i.succ
				end

				next if userRoles.user == "" && userRoles.roles == nil

				userRoles.roles = [] if userRoles.roles == nil
				list << userRoles
			end

			return 0, list
		end

		def parse_roles(userRoles)
			size = @data_buffer.read(@data_offset)
			@data_offset += 1
			userRoles.roles = []

			i = 0
			while i < size
				len = @data_buffer.read(@data_offset)
				@data_offset += 1
				role = @data_buffer.read(@data_offset, len)
				@data_offset += len
				userRoles.roles << role

				i = i.succ
			end
		end
		
		SALT = '$2a$10$7EqJtq98hPqEX7fNZaFWoO'
		def self.hash_password(password)
			# Hashing the password with the cost of 10, with a static salt
			return BCrypt::Engine.hash_secret(password, SALT, :cost => 10)
		end
	end
end

