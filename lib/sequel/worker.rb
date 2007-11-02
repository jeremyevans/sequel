require 'thread'

module Sequel
  class Worker < Thread
    class WorkerStopError < RuntimeError; end
  
    attr_reader :queue
  
    def initialize(db = nil)
      @queue = Queue.new
      t = self
      if db
        super {db.transaction {t.work}}
      else
        super {t.work}
      end
    end
  
    def work
      begin
        loop {@cur = @queue.pop; @cur.call; @cur = nil}
      rescue WorkerStopError # do nothing
      end
    end
    
    def busy?
      @cur || !@queue.empty?
    end
  
    def async(proc = nil, &block)
      @queue << (proc || block)
    end
    alias_method :add, :async
    alias_method :<<, :async
  
    def join
      while busy?
        sleep 0.1
      end
      self.raise WorkerStopError
      super
    end
  end
end