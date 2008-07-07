module Sequel
  # A Worker is a thread that accepts jobs off a work queue and
  # processes them in the background.  It accepts an optional
  # database where it wruns all of its work inside a transaction.
  class Worker < Thread
    attr_reader :queue
    attr_reader :errors
  
    # Setup the interal variables.  If a database is given,
    # run the thread inside a database transaction. Continue
    # to work until #join is called.
    def initialize(db = nil)
      @queue = Queue.new
      @errors = []
      t = self
      t.abort_on_exception = true
      @transaction = !db.nil?
      db ? super {db.transaction {t.work}} : super {t.work}
    end
    
    # Add a job to the queue, specified either as a proc argument
    # or as a block.
    def async(proc = nil, &block)
      @queue << (proc || block)
      self
    end
    alias_method :add, :async
    alias_method :<<, :async
  
    # Whether the worker is actively working and/or has work scheduled
    def busy?
      @cur || !@queue.empty?
    end
  
    # Wait until the worker is no longer busy and then stop working.
    def join
      sleep(0.1) while busy?
      self.raise Error::WorkerStop
      super
    end

    # Continually get jobs from the work queue and process them.
    def work
      begin
        loop {next_job}
      rescue Sequel::Error::WorkerStop # signals the worker thread to stop
      ensure
        raise Sequel::Error::Rollback if @transaction && !@errors.empty?
      end
    end
    
    private

    # Get the next job from the work queue and process it.
    def next_job
      begin
        @cur = @queue.pop
        @cur.call
      rescue Error::WorkerStop => e
        raise e
      rescue Exception => e
        @errors << e
      ensure
        @cur = nil
      end
    end
  end
end
