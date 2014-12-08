require 'aerospike/key'

module Support

  RAND_CHARS = ('a'..'z').to_a.concat(('A'..'Z').to_a).concat(('0'..'9').to_a)
  HOST = "127.0.0.1"
  PORT = 3000

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
end
