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

