require 'aerospike/key'

module Support

  RAND_CHARS = ('a'..'z').to_a.concat(('A'..'Z').to_a).concat(('0'..'9').to_a)

  def self.rand_string(len)
    RAND_CHARS.shuffle[0,len].join
  end

  def self.gen_random_key(len=50, opts = {set: 'test'})
    key_val = rand_string(len)
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

  def self.client
    @client ||= Aerospike::Client.new()
  end

  def self.feature?(feature)
    self.client.supports_feature?(feature.to_s)
  end

  def self.enterprise?
    @enterprise_edition ||=
      begin
        info = self.client.request_info("edition")
        info["edition"] =~ /Enterprise/
      end
  end

  def self.version
    @cluster_version ||=
      begin
        version = self.client.request_info("version")["version"]
        version = version[/\d+(?:.\d+)+(:?-\d+)?(?:-[a-z0-9]{8})?/]
        Gem::Version.new(version).release
      end
  end

  # returns true if the server runs at least the specified minimum version
  # of ASD (e.g. "3.9.1")
  def self.min_version?(version)
    version = Gem::Version.new(version)
    self.version >= version
  end

end
