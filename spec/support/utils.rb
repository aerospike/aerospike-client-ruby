require 'aerospike/key'

module Support

  RAND_CHARS = ('a'..'z').to_a.concat(('A'..'Z').to_a).concat(('0'..'9').to_a)
  HOST = ENV.fetch("AS_HOST", "127.0.0.1")
  PORT = ENV.fetch("AS_PORT", 3000).to_i
  USER = ""
  PASSWORD = ""

  def self.rand_string(len)
    RAND_CHARS.shuffle[0,len].join
  end

  def self.gen_random_key(len=50, opts = {:set => 'test', :key_as_sym => false})
    key_val = rand_string(len)
    key_val = key_val.to_sym if opts[:key_as_sym]
    set_name = opts[:set] || 'test'
    Aerospike::Key.new('test', set_name, key_val)
  end

  def self.delete_set(client, set_name)
    package = "test_utils_delete_record.lua"
    function = <<EOF
function delete_record(record)
  aerospike:remove(record)
end
EOF
    register_task = client.register_udf(function, package, Aerospike::Language::LUA)
    register_task.wait_till_completed or fail "Could not register delete_record UDF to delete set #{set_name}"
    statement = Aerospike::Statement.new("test", set_name)
    execute_task = client.execute_udf_on_query(statement, package, "delete_record")
    execute_task.wait_till_completed
    remove_task = client.remove_udf(package)
    remove_task.wait_till_completed or fail "Could not un-register delete_record UDF to delete set #{set_name}"
  end

  def self.host
    HOST
  end

  def self.port
    PORT
  end

  def self.user
    USER
  end

  def self.password
    PASSWORD
  end

end
