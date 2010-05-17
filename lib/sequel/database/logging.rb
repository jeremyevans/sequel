module Sequel
  class Database
    # ---------------------
    # :section: Methods relating to logging
    # This methods affect relating to the logging of executed SQL.
    # ---------------------

    # Numeric specifying the duration beyond which queries are logged at warn
    # level instead of info level.
    attr_accessor :log_warn_duration

    # Array of SQL loggers to use for this database
    attr_accessor :loggers
    
    # Log a message at level info to all loggers.
    def log_info(message, args=nil)
      log_each(:info, args ? "#{message}; #{args.inspect}" : message)
    end

    # Yield to the block, logging any errors at error level to all loggers,
    # and all other queries with the duration at warn or info level.
    def log_yield(sql, args=nil)
      return yield if @loggers.empty?
      sql = "#{sql}; #{args.inspect}" if args
      start = Time.now
      begin
        yield
      rescue => e
        log_each(:error, "#{e.class}: #{e.message.strip}: #{sql}")
        raise
      ensure
        log_duration(Time.now - start, sql) unless e
      end
    end

    # Remove any existing loggers and just use the given logger.
    def logger=(logger)
      @loggers = Array(logger)
    end

    private
    
    # Log the given SQL and then execute it on the connection, used by
    # the transaction code.
    def log_connection_execute(conn, sql)
      log_yield(sql){conn.send(connection_execute_method, sql)}
    end

    # Log message with message prefixed by duration at info level, or
    # warn level if duration is greater than log_warn_duration.
    def log_duration(duration, message)
      log_each((lwd = log_warn_duration and duration >= lwd) ? :warn : :info, "(#{sprintf('%0.6fs', duration)}) #{message}")
    end

    # Log message at level (which should be :error, :warn, or :info)
    # to all loggers.
    def log_each(level, message)
      @loggers.each{|logger| logger.send(level, message)}
    end

  end
end
