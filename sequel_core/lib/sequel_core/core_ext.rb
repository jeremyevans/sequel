class Array
  # Removes and returns the last member of the array if it is a hash. Otherwise,
  # an empty hash is returned This method is useful when writing methods that
  # take an options hash as the last parameter. For example:
  #
  #   def validate_each(*args, &block)
  #     opts = args.extract_options!
  #     ...
  #   end
  def extract_options!
    last.is_a?(Hash) ? pop : {}
  end
end

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

class Class
  private
    # Define an instance method(s) that class the class method of the
    # same name.
    # Replaces the construct:
    #
    #   class << self
    #     attr_reader *meths
    #   end
    def class_attr_reader(*meths)
      meths.each{|meth| define_method(meth){self.class.send(meth)}}
    end

    # Create an alias for a singleton/class method.
    # Replaces the construct:
    #
    #   class << self
    #     alias_method to, from
    #   end
    def metaalias(to, from)
      metaclass.instance_eval{alias_method to, from}
    end
    
    # Make a singleton/class attribute reader method(s).
    # Replaces the construct:
    #
    #   class << self
    #     attr_reader *meths
    #   end
    def metaattr_reader(*meths)
      metaclass.instance_eval{attr_reader *meths}
    end

    # Make a singleton/class method(s) private.
    # Replaces the construct:
    #
    #   class << self
    #     private *meths
    #   end
    def metaprivate(*meths)
      metaclass.instance_eval{private *meths}
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
