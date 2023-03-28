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

module Aerospike

  private
  # Commands
  AUTHENTICATE      = 0
  CREATE_USER       = 1
  DROP_USER         = 2
  SET_PASSWORD      = 3
  CHANGE_PASSWORD   = 4
  GRANT_ROLES       = 5
  REVOKE_ROLES      = 6
  QUERY_USERS       = 9
  CREATE_ROLE       = 10
  DROP_ROLE         = 11
  GRANT_PRIVILEGES  = 12
  REVOKE_PRIVILEGES = 13
  SET_WHITELIST     = 14
  SET_QUOTAS        = 15
  QUERY_ROLES       = 16
  LOGIN             = 20

  # Field IDs
  USER           = 0
  PASSWORD       = 1
  OLD_PASSWORD   = 2
  CREDENTIAL     = 3
  CLEAR_PASSWORD = 4
  SESSION_TOKEN  = 5
  SESSION_TTL    = 6
  ROLES          = 10
  ROLE           = 11
  PRIVILEGES     = 12
  ALLOWLIST      = 13
  READ_QUOTA     = 14
  WRITE_QUOTA    = 15
  READ_INFO      = 16
  WRITE_INFO     = 17
  CONNECTIONS    = 18

  # Misc
  MSG_VERSION  = 2
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

    def create_role(cluster, policy, role_name, privileges = [], allowlist = [], read_quota = 0, write_quota = 0)
      field_count = 1
      field_count += 1 if privileges.size > 0
      field_count += 1 if allowlist.size > 0
      field_count += 1 if read_quota > 0
      field_count += 1 if write_quota > 0

      write_header(CREATE_ROLE, field_count)
      write_field_str(ROLE, role_name)

      write_privileges(privileges) if privileges.size > 0
      write_allowlist(allowlist) if allowlist.size > 0

      write_field_uint32(READ_QUOTA, read_quota) if read_quota > 0
      write_field_uint32(WRITE_QUOTA, write_quota) if write_quota > 0

      execute_command(cluster, policy)
    end

    def drop_role(cluster, policy, role)
      write_header(DROP_ROLE, 1)
      write_field_str(ROLE, role)
      execute_command(cluster, policy)
    end

    def grant_privileges(cluster, policy, role, privileges)
      write_header(GRANT_PRIVILEGES, 2)
      write_field_str(ROLE, role)
      write_privileges(privileges)
      execute_command(cluster, policy)
    end

    def revoke_privileges(cluster, policy, role, privileges)
      write_header(REVOKE_PRIVILEGES, 2)
      write_field_str(ROLE, role)
      write_privileges(privileges)
      execute_command(cluster, policy)
    end

    def set_allowlist(cluster, policy, role, allowlist = [])
      field_count = 1
      field_count += 1 if allowlist.size > 0
      write_header(SET_WHITELIST, field_count)
      write_allowlist(allowlist) if allowlist.size > 0
      execute_command(cluster, policy)
    end

    def set_quotas(cluster, policy, role, read_quota, write_quota)
      write_header(SET_QUOTAS, 3)
      write_field_str(ROLE, role)
      write_field_uint32(READ_QUOTA, read_quota)
      write_field_uint32(WRITE_QUOTA, write_quota)
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

    def query_role(cluster, policy, role)
      # TODO: Remove the workaround in the future
      sleep(0.010)

      list = []
      begin
        write_header(QUERY_ROLES, 1)
        write_field_str(ROLE, role)
        list = read_roles(cluster, policy)
        return (list.is_a?(Array) && list.length > 0 ? list.first : nil)
      ensure
        Buffer.put(@data_buffer)
      end
    end

    def query_roles(cluster, policy)
      # TODO: Remove the workaround in the future
      sleep(0.010)
      begin
        write_header(QUERY_ROLES, 0)
        return read_roles(cluster, policy)
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

    def write_privileges(privileges)
      offset = @data_offset
      @data_offset += FIELD_HEADER_SIZE
      write_byte(privileges.size)

      for privilege in privileges
        write_byte(privilege.to_code)
        if privilege.can_scope?
          if privilege.set_name.to_s.size > 0 && privilege.namespace.to_s.size == 0
            raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::INVALID_PRIVILEGE, "Admin privilege #{privilege.namespace} has a set scope with an empty namespace")
          end

          write_str(privilege.namespace.to_s)
          write_str(privilege.set_name.to_s)
        else
          if privilege.set_name.to_s.bytesize > 0 || privilege.namespace.to_s.bytesize > 0
            raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::INVALID_PRIVILEGE, "Admin global privilege #{privilege} can't have a namespace or set")
          end
        end
      end

      size = @data_offset - offset - FIELD_HEADER_SIZE
      @data_offset = offset
      write_field_header(PRIVILEGES, size)
      @data_offset += size
    end

    def write_allowlist(allowlist)
      offset = @data_offset
      @data_offset += FIELD_HEADER_SIZE

      comma = false
      for addr in allowlist
        if comma
          write_byte(",")
        else
          comma = true
        end

        @data_offset += @data_buffer.write_binary(addr, @data_offset)
      end

      size = @data_offset - offset - FIELD_HEADER_SIZE
      @data_offset = offset
      write_field_header(ALLOWLIST, size)
      @data_offset += size
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

    def write_byte(b)
      @data_offset += @data_buffer.write_byte(b, @data_offset)
    end

    def write_str(str)
      @data_offset += @data_buffer.write_byte(str.bytesize, @data_offset)
      @data_offset += @data_buffer.write_binary(str, @data_offset)
    end

    def write_field_str(id, str)
      len = @data_buffer.write_binary(str, @data_offset+FIELD_HEADER_SIZE)
      write_field_header(id, len)
      @data_offset += len
    end

    def write_field_uint32(id, val)
      len = @data_buffer.write_uint32(val, @data_offset+FIELD_HEADER_SIZE)
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

      begin
        conn = node.get_connection(timeout)
        conn.write(@data_buffer, @data_offset)
        conn.read(@data_buffer, HEADER_SIZE)
        node.put_connection(conn)
      rescue => e
        node.close_connection(conn) if conn
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
        node.close_connection(conn) if conn
        raise e
      end
      raise Exceptions::Aerospike.new(status) if status > 0
      list
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
      rescue
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

        user_roles = UserRoles.new
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
            user_roles.user = @data_buffer.read(@data_offset, len)
            @data_offset += len
          when ROLES
            parse_roles(user_roles)
          when READ_INFO
            user_roles.read_info = parse_info
          when WRITE_INFO
            user_roles.write_info = parse_info
          when CONNECTIONS
            user_roles.conns_in_use = @data_buffer.read_int32(@data_offset)
            @data_offset += len
          else
            @data_offset += len
          end

          i = i.succ
        end

        next if user_roles.user == "" && user_roles.roles == nil

        user_roles.roles = [] if user_roles.roles == nil
        list << user_roles
      end

      return 0, list
    end

    def parse_roles(user_roles)
      size = @data_buffer.read(@data_offset)
      @data_offset += 1
      user_roles.roles = []

      i = 0
      while i < size
        len = @data_buffer.read(@data_offset)
        @data_offset += 1
        role = @data_buffer.read(@data_offset, len)
        @data_offset += len
        user_roles.roles << role

        i = i.succ
      end
    end

    def parse_info
      size = @data_buffer.read(@data_offset)
      @data_offset += 1
      list = []

      i = 0
      while i < size
        val = @data_buffer.read_int32(@data_offset)
        @data_offset += 4
        list << val

        i = i.succ
      end

      list
    end

    def read_roles(cluster, policy)
      write_size
      node = cluster.random_node

      timeout = 1
      timeout = policy.timeout if policy != nil && policy.timeout > 0

      status = -1
      list = []
      begin
        conn = node.get_connection(timeout)
        conn.write(@data_buffer, @data_offset)
        status, list = read_role_blocks(conn)
        node.put_connection(conn)
      rescue => e
        node.close_connection(conn) if conn
        raise e
      end

      raise Exceptions::Aerospike.new(status) if status > 0

      list
    end

    def read_role_blocks(conn)
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
            status, list = parse_roles_full(receive_size)
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

    def parse_roles_full(receive_size)
      @data_offset = 0
      list = []

      while @data_offset < receive_size
        result_code = @data_buffer.read(@data_offset+1)

        if result_code != 0
          return (result_code == QUERY_END ? -1 : result_code)
        end

        role = Role.new
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
          when ROLE
            role.name = @data_buffer.read(@data_offset, len).to_s
            @data_offset += len
          when PRIVILEGES
            parse_privileges(role)
          when ALLOWLIST
            role.allowlist = parse_allowlist(len)
          when READ_QUOTA
            role.read_quota = @data_buffer.read_uint32(@data_offset)
            @data_offset += len
          when WRITE_QUOTA
            role.write_quota = @data_buffer.read_uint32(@data_offset)
            @data_offset += len
          else
            @data_offset += len
          end

          i = i.succ
        end

        next if role.name == "" && role.privileges == nil

        role.privileges ||= []
        list << role
      end

      return 0, list
    end

    def parse_privileges(role)
      size = @data_buffer.read(@data_offset)
      @data_offset += 1
      role.privileges = []

      i = 0
      while i < size
        priv = Privilege.new
        priv.code = Privilege.from(@data_buffer.read(@data_offset))
        @data_offset += 1

        if priv.can_scope?
          len = @data_buffer.read(@data_offset)
          @data_offset += 1
          priv.namespace = @data_buffer.read(@data_offset, len)
          @data_offset += len

          len = @data_buffer.read(@data_offset)
          @data_offset += 1
          priv.set_name = @data_buffer.read(@data_offset, len)
          @data_offset += len
        end

        role.privileges << priv

        i = i.succ
      end
    end

    def parse_allowlist(len)
      list = []
      begn = @data_offset
      max = begn + len

      while @data_offset < max
        if @data_buffer.read(@data_offset) == ','
          l = @data_offset - begn
          if l > 0
            s = @data_buffer.read(begn, l)
            list << s
          end
          @data_offset += 1
          begn = @data_offset
        else
          @data_offset += 1
        end
      end

      l = @data_offset - begn
      if l > 0
        s = @data_buffer.read(begn, l)
        list << s
      end

      list
    end

  end
end

