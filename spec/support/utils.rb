require 'aerospike/key'

module Support

  RAND_CHARS = ('a'..'z').to_a.concat(('A'..'Z').to_a).concat(('0'..'9').to_a)
  HOST = "127.0.0.1"
  PORT = 3000
  USER = ""
  PASSWORD = ""

  def self.rand_string(len)
    RAND_CHARS.shuffle[0,len].join
  end

  def self.gen_random_key(len=50, opts = {:key_as_sym => false})
    key_val = rand_string(len)
    key_val = key_val.to_sym if opts[:key_as_sym]
    Aerospike::Key.new('test', 'test', key_val)
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
