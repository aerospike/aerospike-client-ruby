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

require "spec_helper"
require "aerospike/query/statement"

describe Aerospike::Client do

  if Support.user != ''

    describe "Security operations" do

      before :all do
        @client = described_class.new(Support.host, Support.port, :user => Support.user, :password => Support.password)
      end

      after :all do
        @client.close
      end

      context "Roles" do

        before :each do
          begin
            @client.drop_user('test_user')
          rescue
          end
      
          @client.create_user('test_user', 'test', ["user-admin"])
        end

        it "must query roles perfectly" do
          admin = @client.query_user('test_user')

          expect(admin.user).to eq 'test_user'
          expect(admin.roles).to include 'user-admin'
        end # it

        it "Must Revoke/Grant Roles Perfectly" do
          @client.grant_roles("test_user", ["user-admin", "sys-admin", "read-write", "read"])
          admin = @client.query_user("test_user")

          expect(admin.user).to eq "test_user"
          expect(admin.roles).to match_array(["user-admin", "sys-admin", "read-write", "read"])
          
          @client.revoke_roles("test_user", ["sys-admin"])
          admin = @client.query_user("test_user")

          expect(admin.user).to eq "test_user"
          expect(admin.roles).to match_array(["user-admin", "read-write", "read"])
        end

        it "Must Replace Roles Perfectly" do        
          @client.replace_roles("test_user", ["user-admin", "read"])
          admin = @client.query_user("test_user")

          expect(admin.user).to eq "test_user"
          expect(admin.roles).to match_array(["user-admin", "read"])
        end

      end # context

      context "Users" do

        it "Must Create/Drop User" do
          # drop before test
          @client.drop_user("test_user")

          @client.create_user("test_user", "test", ["user-admin", "read"])

          admin = @client.query_user("test_user")

          expect(admin.user).to eq "test_user"
          expect(admin.roles).to match_array(["user-admin", "read"])
        end

        it "Must Change User Password" do
          # drop before test
          @client.drop_user("test_user")

          @client.create_user("test_user", "test", ["user-admin", "read"])

          # connect using the new user
          new_client = Aerospike::Client.new(Support.host, Support.port, :user => 'test_user', :password => 'test')

          # change current user's password on the fly
          new_client.change_password("test_user", "test1")

          # exhaust all node connections
          new_client.nodes.each do |node|
            for i in 0...(Aerospike::ClientPolicy.new.connection_queue_size)
              conn = node.get_connection(1)
                  conn.close if conn
            end
          end

          # should have the password changed in the cluster, so that a new connection
          # will be established and used
          admin = new_client.query_user("test_user")

          expect(admin.user).to eq "test_user"
          expect(admin.roles).to match_array(["user-admin", "read"])
          new_client.close
        end

        it "Must Query all users" do
          user_count = 10

          # drop before test
          for i in 1...user_count
            begin
              @client.drop_user("test_user#{i}")
            rescue
            end
          end

          for i in 1...user_count
            @client.create_user("test_user#{i}", "test", ["read"])
            end

          # should have the password changed in the cluster, so that a new connection
          # will be established and used
          users = @client.query_users
          expect(users.length).to be >= user_count-1

          for i in 1...user_count
            @client.drop_user("test_user#{i}")
          end

        end

      end # describe users

    end # describe

  end # if

end # describe
