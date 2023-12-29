# frozen-string-literal: true
#
# The auto_cast_date_and_time extension uses SQL standard type casting
# when literalizing date, time, and timestamp values:
#
#   DB.literal(Time.now)
#   # => "TIMESTAMP '...'"
#
#   DB.literal(Date.today)
#   # => "DATE '...'"
#
#   DB.literal(Sequel::SQLTime.create(10, 20, 30))
#   # => "TIME '10:20:30.000000'"
#
# The default behavior of Sequel on adapters that do not require the
# SQL standard behavior is to format the date or time value without:
# casting
#
#   DB.literal(Sequel::SQLTime.create(10, 20, 30))
#   # => "'10:20:30.000000'"
#
# However, then the database cannot determine the type of the string,
# and must perform some implicit casting.  If implicit casting cannot
# be used, it will probably treat the value as a string:
#
#  DB.get(Time.now).class
#  # Without auto_cast_date_and_time: String
#  #    With auto_cast_date_and_time: Time
#
# Note that not all databases support this extension. PostgreSQL and
# MySQL support it, but SQLite and Microsoft SQL Server do not.
#
# You can load this extension into specific datasets:
#
#   ds = DB[:table]
#   ds = ds.extension(:auto_cast_date_and_time)
#
# Or you can load it into all of a database's datasets, which
# is probably the desired behavior if you are using this extension:
#
#   DB.extension(:auto_cast_date_and_time)
# 
# Related module: Sequel::AutoCastDateAndTime

#
module Sequel
  module AutoCastDateAndTime
    # :nocov:

    # Mark the datasets as requiring sql standard date times.  This is only needed
    # for backwards compatibility.  
    def requires_sql_standard_datetimes?
      # SEQUEL6: Remove
      true
    end
    # :nocov:

    private

    # Explicitly cast SQLTime objects to TIME.
    def literal_sqltime_append(sql, v)
      sql << "TIME "
      super
    end

    # Explicitly cast Time objects to TIMESTAMP.
    def literal_time_append(sql, v)
      sql << literal_datetime_timestamp_cast
      super
    end

    # Explicitly cast DateTime objects to TIMESTAMP.
    def literal_datetime_append(sql, v)
      sql << literal_datetime_timestamp_cast
      super
    end

    # Explicitly cast Date objects to DATE.
    def literal_date_append(sql, v)
      sql << "DATE "
      super
    end

    # The default cast string to use for Time/DateTime objects.
    # Respects existing method if already defined.
    def literal_datetime_timestamp_cast
      return super if defined?(super)
      'TIMESTAMP '
    end
  end

  Dataset.register_extension(:auto_cast_date_and_time, AutoCastDateAndTime)
end

