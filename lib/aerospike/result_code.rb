# encoding: utf-8
# Copyright 2014-2020 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License") you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

module Aerospike

  module ResultCode

    attr_reader :code

    # One or more keys failed in a batch.
    BATCH_FAILED = -20

    # No response was received from the server.
    NO_RESPONSE = -19

    # A network error. Checked the wrapped error for detail.
    NETWORK_ERROR = -18

    # A common, none-aerospike error. Checked the wrapped error for detail.
    COMMON_ERROR = -17

    # Max retries limit reached.
    MAX_RETRIES_EXCEEDED = -16

    # Max errors limit reached.
    MAX_ERROR_RATE = -15

    # Requested Rack for node/namespace was not defined in the cluster.
    RACK_NOT_DEFINED = -13

    # Cluster has an invalid partition map, usually due to bad configuration.
    INVALID_CLUSTER_PARTITION_MAP = -12

    # Server is not accepting requests.
    SERVER_NOT_AVAILABLE = -11

    # Cluster Name does not match the ClientPolicy.ClusterName value.
    CLUSTER_NAME_MISMATCH_ERROR = -10

    # Recordset has already been closed or cancelled
    RECORDSET_CLOSED = -9

    # There were no connections available to the node in the pool, and the pool was limited
    NO_AVAILABLE_CONNECTIONS_TO_NODE = -8

    # Value type not supported by Aerospike server
    TYPE_NOT_SUPPORTED = -7

    # Asynchronous max concurrent database commands have been exceeded and therefore rejected.
    COMMAND_REJECTED = -6

    # Query was terminated by user.
    QUERY_TERMINATED = -5

    # Scan was terminated by user.
    SCAN_TERMINATED = -4

    # Chosen node is not currently active.
    INVALID_NODE_ERROR = -3

    # Client parse error.
    PARSE_ERROR = -2

    # Client serialization error.
    SERIALIZE_ERROR = -1

    # Operation was successful.
    OK = 0

    # Unknown server failure.
    SERVER_ERROR = 1

    # On retrieving, touching or replacing a record that doesn't exist.
    KEY_NOT_FOUND_ERROR = 2

    # On modifying a record with unexpected generation.
    GENERATION_ERROR = 3

    # Bad parameter(s) were passed in database operation call.
    PARAMETER_ERROR = 4

    # On create-only (write unique) operations on a record that already
    # exists.
    KEY_EXISTS_ERROR = 5

    # Bin already exists on a create-only operation.
    BIN_EXISTS_ERROR = 6


    # Expected cluster ID was not received.
    CLUSTER_KEY_MISMATCH = 7

    # Server has run out of memory.
    SERVER_MEM_ERROR = 8

    # Client or server has timed out.
    TIMEOUT = 9

    # XDS product is not available.
    NO_XDS = 10

    # Partition is unavailable.
    PARTITION_UNAVAILABLE = 11

    # Operation is not supported with configured bin type (single-bin or
    # multi-bin).
    BIN_TYPE_ERROR = 12

    # Record size exceeds limit.
    RECORD_TOO_BIG = 13

    # Too many concurrent operations on the same record.
    KEY_BUSY = 14

    # Scan aborted by server.
    SCAN_ABORT = 15

    # Unsupported Server Feature (e.g. Scan + UDF)
    UNSUPPORTED_FEATURE = 16

    # Bin not found on update-only operation.
    BIN_NOT_FOUND = 17

    # Specified bin name does not exist in record.
    DEVICE_OVERLOAD = 18

    # Key type mismatch.
    KEY_MISMATCH = 19

    # Invalid namespace.
    INVALID_NAMESPACE = 20

    # Sent too-long bin name (>15, should be impossible in this client) or exceeded
    # namespace's bin name quota.
    BIN_NAME_TOO_LONG = 21

    # Operation not allowed at this time.
    FAIL_FORBIDDEN = 22

    # Returned by Map put and put_items operations when policy is REPLACE but key was not found
    ELEMENT_NOT_FOUND = 23

    # Returned by Map put and put_items operations when policy is CREATE_ONLY but key already exists
    ELEMENT_EXISTS = 24

    # Enterprise-only feature not supported by the community edition
    ENTERPRISE_ONLY = 25

    # The operation cannot be applied to the current bin value on the server.
    OP_NOT_APPLICABLE = 26

    # The transaction was not performed because the predexp was false.
    FILTERED_OUT = 27

    # Write command loses conflict to XDR.
    LOST_CONFLICT = 28

    # There are no more records left for query.
    QUERY_END = 50

    # Security functionality not supported by connected server.
    SECURITY_NOT_SUPPORTED = 51

    # Security functionality not enabled by connected server.
    SECURITY_NOT_ENABLED = 52

    # Security scheme not supported.
    SECURITY_SCHEME_NOT_SUPPORTED = 53

    # Administration command is invalid.
    INVALID_COMMAND = 54

    # Administration field is invalid.
    INVALID_FIELD = 55

    ILLEGAL_STATE = 56

    # User name is invalid.
    INVALID_USER = 60

    # User was previously created.
    USER_ALREADY_EXISTS = 61

    # Password is invalid.
    INVALID_PASSWORD = 62

    # Password is invalid.
    EXPIRED_PASSWORD = 63

    # Password is invalid.
    FORBIDDEN_PASSWORD = 64

    # Security credential is invalid.
    INVALID_CREDENTIAL = 65

    # Expired session token.
    EXPIRED_SESSION = 66

    # Role name is invalid.
    INVALID_ROLE = 70

    # Role Already exists
    ROLE_ALREADY_EXISTS = 71

    # Privilege is invalid.
    INVALID_PRIVILEGE = 72

    # Specified IP allowlist is invalid.
    INVALID_WHITELIST = 73

    # User must be authentication before performing database operations.
    NOT_AUTHENTICATED = 80

    # User does not posses the required role to perform the database operation.
    ROLE_VIOLATION = 81

    # Client IP address is not on the IP allowlist.
    NOT_WHITELISTED = 82

    # LDAP feature not enabled on server.
    LDAP_NOT_ENABLED = 90

    # Error in LDAP setup.
    LDAP_SETUP = 91

    # Error in LDAP TLS setup.
    LDAP_TLS_SETUP = 92

    # Error authenticating LDAP user.
    LDAP_AUTHENTICATION = 93

    # Error querying LDAP server.
    LDAP_QUERY = 94

    # A user defined function returned an error code.
    UDF_BAD_RESPONSE = 100

    # Batch functionality has been disabled by configuring the batch-index-thread=0.
    BATCH_DISABLED = 150

    # Batch max requests has been exceeded.
    BATCH_MAX_REQUESTS = 151

    # All batch queues are full.
    BATCH_QUEUES_FULL = 152

    # GeoJSON is malformed or not supported.
    INVALID_GEOJSON = 160

    # Secondary index already exists.
    INDEX_FOUND = 200

    # Requested secondary index does not exist.
    INDEX_NOTFOUND = 201

    # Secondary index memory space exceeded.
    INDEX_OOM = 202

    # Secondary index not available.
    INDEX_NOTREADABLE = 203

    # Generic secondary index error.
    INDEX_GENERIC = 204

    # Index name maximum length exceeded.
    INDEX_NAME_MAXLEN = 205

    # Maximum number of indicies exceeded.
    INDEX_MAXCOUNT = 206

    # Secondary index query aborted.
    QUERY_ABORTED = 210

    # Secondary index queue full.
    QUERY_QUEUEFULL = 211

    # Secondary index query timed out on server.
    QUERY_TIMEOUT = 212

    # Generic query error.
    QUERY_GENERIC = 213

    # Network error. Query is aborted.
    QUERY_NET_IO = 214

    # Internal error.
    QUERY_DUPLICATE = 215

    def self.message(code)
      case code
      when BATCH_FAILED
          "one or more keys failed in a batch"

      when NO_RESPONSE
          "no response was received from the server"

      when NETWORK_ERROR
          "network error. Checked the wrapped error for detail"

      when COMMON_ERROR
          "common, none-aerospike error. Checked the wrapped error for detail"

      when MAX_RETRIES_EXCEEDED
          "Max retries exceeded"

      when MAX_ERROR_RATE
          "Max errors limit reached for node"

      when RACK_NOT_DEFINED
          "Requested Rack for node/namespace was not defined in the cluster."

      when INVALID_CLUSTER_PARTITION_MAP
          "Cluster has an invalid partition map, usually due to bad configuration."

      when SERVER_NOT_AVAILABLE
          "Server is not accepting requests."

      when CLUSTER_NAME_MISMATCH_ERROR
          "Cluster Name does not match the ClientPolicy.ClusterName value"

      when RECORDSET_CLOSED
          "Recordset has already been closed or cancelled."

      when NO_AVAILABLE_CONNECTIONS_TO_NODE
          "No available connections to the node. Connection Pool was empty, and limited to certain number of connections."

      when TYPE_NOT_SUPPORTED
          "Type cannot be converted to Value Type."

      when COMMAND_REJECTED
        "Command rejected"

      when QUERY_TERMINATED
        "Query terminated"

      when SCAN_TERMINATED
        "Scan terminated"

      when INVALID_NODE_ERROR
        "Invalid node"

      when PARSE_ERROR
        "Parse error"

      when SERIALIZE_ERROR
        "Serialize error"

      when OK
        "ok"

      when SERVER_ERROR
        "Server error"

      when KEY_NOT_FOUND_ERROR
        "Key not found"

      when GENERATION_ERROR
        "Generation error"

      when PARAMETER_ERROR
        "Parameter error"

      when KEY_EXISTS_ERROR
        "Key already exists"

      when BIN_EXISTS_ERROR
        "Bin already exists on a create-only operation"

      when CLUSTER_KEY_MISMATCH
        "Cluster key mismatch"

      when SERVER_MEM_ERROR
        "Server memory error"

      when TIMEOUT
        "Timeout"

      when NO_XDS
        "XDS not available"

      when PARTITION_UNAVAILABLE
          "Partition not available"

      when BIN_TYPE_ERROR
        "Bin type error"

      when RECORD_TOO_BIG
        "Record too big"

      when KEY_BUSY
        "Hot key"

      when SCAN_ABORT
        "Scan aborted"

      when UNSUPPORTED_FEATURE
        "Unsupported Server Feature"

      when BIN_NOT_FOUND
        "Bin not found on update-only operation"

      when DEVICE_OVERLOAD
        "Device overload"

      when KEY_MISMATCH
        "Key mismatch"

      when INVALID_NAMESPACE
        "Invalid namespace"

      when BIN_NAME_TOO_LONG
        "Sent too-long bin name or exceeded namespace's bin name quota."

      when FAIL_FORBIDDEN
        "Operation not allowed at this time"

      when ELEMENT_NOT_FOUND
        "Element not found"

      when ELEMENT_EXISTS
        "Element already exists"

      when ENTERPRISE_ONLY
        "Enterprise-only feature not supported by community edition"

      when OP_NOT_APPLICABLE
        "The operation cannot be applied to the current bin value on the server."

      when FILTERED_OUT
        "The transaction was not performed because the predexp was false."

      when LOST_CONFLICT
        "Write command loses conflict to XDR."

      when QUERY_END
        "Query end"

      when SECURITY_NOT_SUPPORTED
        "Security not supported"

      when SECURITY_NOT_ENABLED
        "Security not enabled"

      when SECURITY_SCHEME_NOT_SUPPORTED
        "Security scheme not supported"

      when INVALID_COMMAND
        "Invalid command"

      when INVALID_FIELD
        "Invalid field"

      when ILLEGAL_STATE
        "Illegal state"

      when INVALID_USER
        "Invalid user"

      when USER_ALREADY_EXISTS
        "User already exists"

      when INVALID_PASSWORD
        "Invalid password"

      when EXPIRED_PASSWORD
        "Expired password"

      when FORBIDDEN_PASSWORD
        "Forbidden password"

      when INVALID_CREDENTIAL
        "Invalid credential"

      when EXPIRED_SESSION
        "Expired session token"

      when INVALID_ROLE
        "Invalid role"

      when ROLE_ALREADY_EXISTS
        "Role already exists"

      when INVALID_PRIVILEGE
        "Invalid privilege"

      when INVALID_WHITELIST
        "Specified IP allowlist is invalid"

      when NOT_AUTHENTICATED
        "Not authenticated"

      when ROLE_VIOLATION
        "Role violation"

      when NOT_WHITELISTED
        "Client IP address is not on the IP allowlist"

      when LDAP_NOT_ENABLED
        "LDAP feature not enabled on server"

      when LDAP_SETUP
        "Error in LDAP setup"

      when LDAP_TLS_SETUP
        "Error in LDAP TLS setup"

      when LDAP_AUTHENTICATION
        "Error authenticating LDAP user"

      when LDAP_QUERY
        "Error querying LDAP server"

      when UDF_BAD_RESPONSE
        "UDF d error"

      when BATCH_DISABLED
        "Batch functionality has been disabled by configuring the batch-index-thread=0"

      when BATCH_MAX_REQUESTS
        "Batch max requests has been exceeded"

      when BATCH_QUEUES_FULL
        "All batch queues are full"

      when INVALID_GEOJSON
        "GeoJSON is malformed or not supported"

      when INDEX_FOUND
        "Index already exists"

      when INDEX_NOTFOUND
        "Index not found"

      when INDEX_OOM
        "Index out of memory"

      when INDEX_NOTREADABLE
        "Index not readable"

      when INDEX_GENERIC
        "Index error - check server logs"

      when INDEX_NAME_MAXLEN
        "Index name max length exceeded"

      when INDEX_MAXCOUNT
        "Index count exceeds max"

      when QUERY_ABORTED
        "Query aborted"

      when QUERY_QUEUEFULL
        "Query queue full"

      when QUERY_TIMEOUT
        "Query timeout"

      when QUERY_GENERIC
        "Query error"

      when QUERY_NET_IO
        "Network error. Query is aborted"

      when QUERY_DUPLICATE
        "Internal query error"

      else
        "ResultCode #{code} unknown in the client. Please file a github issue."
      end # case

    end

  end # class

end # module
