# Time extensions.
class Time
  SQL_FORMAT = "TIMESTAMP '%Y-%m-%d %H:%M:%S'".freeze
  
  # Formats the Time object as an SQL TIMESTAMP.
  def to_sql_timestamp
    strftime(SQL_FORMAT)
  end
end

# Enumerable extensions.
module Enumerable
  def send_each(sym, *args)
    each {|i| i.send(sym, *args)}
  end
end