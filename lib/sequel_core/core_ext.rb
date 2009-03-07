class FalseClass
  # false is always blank
  def blank?
    true
  end
end

# Helpers from Metaid and a bit more
class Object
  # Objects are blank if they respond true to empty?
  def blank?
    respond_to?(:empty?) && empty?
  end
end

class NilClass
  # nil is always blank
  def blank?
    true
  end
end

class Numeric
  # Numerics are never blank (not even 0)
  def blank?
    false
  end
end

class String
  # Strings are blank if they are empty or include only whitespace
  def blank?
    strip.empty?
  end

  # Converts a string into a Date object.
  def to_date
    begin
      Date.parse(self, Sequel.convert_two_digit_years)
    rescue => e
      raise Sequel::Error::InvalidValue, "Invalid Date value '#{self}' (#{e.message})"
    end
  end

  # Converts a string into a DateTime object.
  def to_datetime
    begin
      DateTime.parse(self, Sequel.convert_two_digit_years)
    rescue => e
      raise Sequel::Error::InvalidValue, "Invalid DateTime value '#{self}' (#{e.message})"
    end
  end

  # Converts a string into a Time or DateTime object, depending on the
  # value of Sequel.datetime_class
  def to_sequel_time
    begin
      if Sequel.datetime_class == DateTime
        DateTime.parse(self, Sequel.convert_two_digit_years)
      else
        Sequel.datetime_class.parse(self)
      end
    rescue => e
      raise Sequel::Error::InvalidValue, "Invalid #{Sequel.datetime_class} value '#{self}' (#{e.message})"
    end
  end

  # Converts a string into a Time object.
  def to_time
    begin
      Time.parse(self)
    rescue => e
      raise Sequel::Error::InvalidValue, "Invalid Time value '#{self}' (#{e.message})"
    end
  end
end

class TrueClass
  # true is never blank
  def blank?
    false
  end
end
