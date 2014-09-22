require 'thread'
require 'timeout'

module Apik

  class Pool

    attr_accessor :create_block, :cleanup_block

    def initialize(max_size = 128, &block)
      @create_block = block
      @cleanup_block = nil

      @pool = Queue.new
      @max_size = max_size
    end

    def offer(obj)
      if @pool.length < @max_size
        @pool << obj
      elsif @cleanup_block
        @cleanup_block.call(obj)
      end
    end
    alias_method :<<, :offer

    def poll
      res = nil
      begin
        res = @pool.pop(true) # non_blocking
        return res
      rescue
        return @create_block.call
      end
    end

    def finalizer=(&block)
      raise ArgumentError, "shutdown must receive a block" unless block_given?

      @mutex.synchronize do
        @cleanup_block = block
      end
    end

    def empty?
      @pool.length == 0
    end

    def length
      @pool.length
    end

  end

end
