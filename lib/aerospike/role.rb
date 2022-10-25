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

  # Role provides granular access to database entities for users.
  class Role

    # Role name
    attr_accessor :name

    # List of assigned privileges
    attr_accessor :privileges

    # List of allowable IP addresses
    attr_accessor :allowlist

    # Maximum reads per second limit for the role
    attr_accessor :read_quota

    # Maximum writes per second limit for the role
    attr_accessor :write_quota

    # The following aliases are for backward compatibility reasons
    USER_ADMIN = Privilege::USER_ADMIN # :nodoc:
    SYS_ADMIN = Privilege::SYS_ADMIN # :nodoc:
    DATA_ADMIN = Privilege::DATA_ADMIN # :nodoc:
    UDF_ADMIN = Privilege::UDF_ADMIN # :nodoc:
    SINDEX_ADMIN = Privilege::SINDEX_ADMIN # :nodoc:
    READ_WRITE_UDF = Privilege::READ_WRITE_UDF # :nodoc:
    READ_WRITE = Privilege::READ_WRITE # :nodoc:
    READ = Privilege::READ # :nodoc:
    WRITE = Privilege::WRITE # :nodoc:
    TRUNCATE = Privilege::TRUNCATE # :nodoc:

    def to_s
      "Role [name=#{@name}, privileges=#{@privileges}, allowlist=#{@allowlist}, readQuota=#{@read_quota}, writeQuota=#{@write_quota}]";
    end

  end # class

end # module
