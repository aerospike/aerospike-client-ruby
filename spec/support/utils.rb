require 'aerospike/key'

module Support

  RAND_CHARS = ('a'..'z').to_a.concat(('A'..'Z').to_a).concat(('0'..'9').to_a)
  HOST = "172.16.224.135"
  PORT = 3000
  USER = ""
  PASSWORD = ""

  def self.rand_string(len)
    RAND_CHARS.shuffle[0,len].join
  end

  def self.gen_random_key(len=50)
    Aerospike::Key.new('test', 'test', rand_string(len))
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
