# Module containing overrides for Sequel's standard date/time literalization
# to use the SQL standrd.  The SQL standard is used by fewer databases than
# the defacto standard (which is just a normal string).
module Sequel::Dataset::SQLStandardDateFormat
  private

  # Use SQL standard syntax for Date
  def literal_date(v)
    v.strftime("DATE '%Y-%m-%d'") 
  end
    
  # Use SQL standard syntax for DateTime
  def literal_datetime(v)
    v.strftime("TIMESTAMP '%Y-%m-%d %H:%M:%S'")
  end

  # Use SQL standard syntax for Time
  def literal_time(v)
    v.strftime("TIMESTAMP '%Y-%m-%d %H:%M:%S'")
  end
end
