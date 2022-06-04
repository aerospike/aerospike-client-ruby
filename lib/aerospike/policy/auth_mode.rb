# encoding: utf-8
# Copyright 2014-2020 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Aerospike

  module AuthMode

    # INTERNAL uses internal authentication only when user/password defined. Hashed password is stored
    # on the server. Do not send clear password. This is the default.
    INTERNAL = 0

    # EXTERNAL uses external authentication (like LDAP) when user/password defined. Specific external authentication is
    # configured on server. If TLS is defined, sends clear password on node login via TLS.
    # Will raise exception if TLS is not defined.
    EXTERNAL = 1

    # PKI allows authentication and authorization based on a certificate. No user name or
    # password needs to be configured. Requires TLS and a client certificate.
    # Requires server version 5.7.0+
    PKI = 2

  end # module

end # module
