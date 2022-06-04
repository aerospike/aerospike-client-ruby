# encoding: utf-8
# Copyright 2014-2022 Aerospike, Inc.
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

	# Determines user access granularity.
	class Privilege

		# Role
		attr_accessor :code

		# Namespace determines namespace scope. Apply permission to this namespace only.
		# If namespace is zero value, the privilege applies to all namespaces.
		attr_accessor :namespace

		# Set name scope. Apply permission to this set within namespace only.
		# If set is zero value, the privilege applies to all sets within namespace.
		attr_accessor :set_name

	    # Manage users and their roles.
	    USER_ADMIN = 'user-admin'

	    # Manage indicies, user-defined functions and server configuration.
	    SYS_ADMIN = 'sys-admin'

	    # Manage indicies and user defined functions.
	    DATA_ADMIN = 'data-admin'

	    # Manage user defined functions.
	    UDF_ADMIN = 'udf-admin'

	    # Manage indicies.
	    SINDEX_ADMIN = 'sindex-admin'

	    # Allow read, write and UDF transactions with the database.
	    READ_WRITE_UDF = "read-write-udf"

	    # Allow read and write transactions with the database.
	    READ_WRITE = 'read-write'

	    # Allow read transactions with the database.
	    READ = 'read'

	    # Write allows write transactions with the database.
	    WRITE = 'write'

	    # Truncate allow issuing truncate commands.
	    TRUNCATE = 'truncate'

	    def initialize(opt={})
	      @code = opt[:code]
	      @namespace = opt[:namespace]
	      @set_name = opt[:set_name]
	    end

		def to_s
			"code: #{@code}, namespace: #{@namespace}, set_name: #{@set_name}"
		end

		def to_code
			case @code
			when USER_ADMIN
				0
			when SYS_ADMIN
				1
			when DATA_ADMIN
				2
			when UDF_ADMIN
				3
			when SINDEX_ADMIN
				4
			when READ
				10
			when READ_WRITE
				11
			when READ_WRITE_UDF
				12
			when WRITE
				13
			when TRUNCATE
				14
			else
				raise Exceptions::Aerospike.new(Aerospike::ResultCode::INVALID_PRIVILEGE, "Invalid role #{@code}")
			end # case
		end # def

		def self.from(code)
			case code
			when 0
				USER_ADMIN
			when 1
				SYS_ADMIN
			when 2
				DATA_ADMIN
			when 3
				UDF_ADMIN
			when 4
				SINDEX_ADMIN
			when 10
				READ
			when 11
				READ_WRITE
			when 12
				READ_WRITE_UDF
			when 13
				WRITE
			when 14
				TRUNCATE
			else
				raise Exceptions::Aerospike.new(Aerospike::ResultCode::INVALID_PRIVILEGE, "Invalid code #{code}")
			end # case
		end # def

		def can_scope?
			to_code >= 10
		end

	end # class

end