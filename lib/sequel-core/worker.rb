require 'thread'

module Sequel
  class Worker < Thread
    class WorkerStopError < RuntimeError; end
  
    attr_reader :queue
    attr_reader :errors
  
    def initialize(db = nil)
      @queue = Queue.new
      @errors = []
      t = self
      t.abort_on_exception = true
      @transaction = !db.nil?
      db ? super {db.transaction {t.work}} : super {t.work}
    end
    
    def work
      loop {next_job}
    rescue WorkerStopError # signals the worker thread to stop
    ensure
      rollback! unless @errors.empty?
    end
    
    def busy?
      @cur || !@queue.empty?
    end
  
    def async(proc = nil, &block)
      @queue << (proc || block)
      self
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

    private
    def next_job
      @cur = @queue.pop
      @cur.call
    rescue WorkerStopError => e
      raise e
    rescue Exception => e
      @errors << e
    ensure
      @cur = nil
    end
  end
end