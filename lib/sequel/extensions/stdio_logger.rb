# frozen-string-literal: true
#
# The stdio_logger extension exposes a Sequel::StdioLogger class that
# can be used for logging with Sequel, as a minimal alternative to
# the logger library.  It exposes debug/info/warn/error methods for the
# different warning levels.  The debug method is a no-op, so that setting
# the Database sql_log_level to debug will result in no output for normal
# queries. The info/warn/error methods log the current time, log level,
# and the given message.
# 
# To use this extension:
#
#   Sequel.extension :stdio_logger
#
# Then you you can use Sequel::StdioLogger to wrap IO objects that you
# would like Sequel to log to:
#
#   DB.loggers << Sequel::StdioLogger.new($stdout)
#
#   log_file = File.open("db_queries.log", 'a')
#   log_file.sync = true
#   DB.loggers << Sequel::StdioLogger.new(log_file)
#
# This is implemented as a global extension instead of a Database extension
# because Database loggers must be set before Database extensions are loaded.
#
# Related module: Sequel::StdioLogger

#
module Sequel
  class StdioLogger
    def initialize(device)
      @device = device
    end

    # Do not log debug messages. This is so setting the Database
    # sql_log_level to debug will result in no output.
    def debug(msg)
    end

    [:info, :warn, :error].each do |meth|
      define_method(meth) do |msg|
        @device.write("#{Time.now.strftime('%F %T')} #{meth.to_s.upcase}: #{msg}\n")
        nil
      end
    end
  end
end
