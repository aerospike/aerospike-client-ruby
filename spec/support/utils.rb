module Support

  RAND_CHARS = ('a'..'z').to_a.concat(('A'..'Z').to_a).concat(('0'..'9').to_a)

  def self.rand_string(len)
    RAND_CHARS.shuffle[0,len].join
  end

end
