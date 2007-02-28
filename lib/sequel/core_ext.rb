# Time extensions.
class Time
  SQL_FORMAT = "TIMESTAMP '%Y-%m-%d %H:%M:%S'".freeze
  
  # Formats the Time object as an SQL TIMESTAMP.
  def to_sql_timestamp
    strftime(SQL_FORMAT)
  end
end
