# Enumerable extensions.
module Enumerable
  # Invokes the specified method for each item, along with the supplied
  # arguments.
  def send_each(sym, *args)
    each {|i| i.send(sym, *args)}
  end
end

# Range extensions
class Range
  # Returns the interval between the beginning and end of the range.
  def interval
    last - first
  end
end

# Object extensions
class Object
  def is_one_of?(*classes)
    classes.each {|c| return c if is_a?(c)}
    nil
  end
end

module Sequel
  # Facilitates time calculations by providing methods to convert from larger
  # time units to seconds, and to convert relative time intervals to absolute
  # ones. This module duplicates some of the functionality provided by Rails'
  # ActiveSupport::CoreExtensions::Numeric::Time module.
  module NumericExtensions
    MINUTE = 60
    HOUR = 3600
    DAY = 86400
    WEEK = DAY * 7
  
    # Converts self from minutes to seconds
    def minutes;  self * MINUTE;  end; alias_method :minute, :minutes
    # Converts self from hours to seconds
    def hours;    self * HOUR;    end; alias_method :hour, :hours
    # Converts self from days to seconds
    def days;     self * DAY;     end; alias_method :day, :days
    # Converts self from weeks to seconds
    def weeks;    self * WEEK;    end; alias_method :week, :weeks
  
    # Returns the time at now - self.
    def ago(t = Time.now); t - self; end
    alias_method :before, :ago

    # Returns the time at now + self.
    def from_now(t = Time.now); t + self; end
    alias_method :since, :from_now
    
    # Extends the Numeric class with numeric extensions.
    def self.enable
      Numeric.send(:include, self)
    end
  end
end
