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
  # Returns true if the object is a object of one of the classes
  def is_one_of?(*classes)
    classes.each {|c| return c if is_a?(c)}
    nil
  end

  # Objects are blank if they respond true to empty?
  def blank?
    nil? || (respond_to?(:empty?) && empty?)
  end
end

class Numeric
  # Numerics are never blank (not even 0)
  def blank?
    false
  end
end

class NilClass
  # nil is always blank
  def blank?
    true
  end
end

class TrueClass
  # true is never blank
  def blank?
    false
  end
end

class FalseClass
  # false is always blank
  def blank?
    true
  end
end

class String
  BLANK_STRING_REGEXP = /\A\s*\z/
  # Strings are blank if they are empty or include only whitespace
  def blank?
    empty? || BLANK_STRING_REGEXP.match(self)
  end
end
