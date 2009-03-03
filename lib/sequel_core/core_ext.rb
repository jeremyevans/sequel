class Array
  # True if the array is not empty and all of its elements are
  # arrays of size 2.  This is used to determine if the array
  # could be a specifier of conditions, used similarly to a hash
  # but allowing for duplicate keys.
  #
  #    hash.to_a.all_two_pairs? # => true unless hash is empty
  def all_two_pairs?
    !empty? && all?{|i| (Array === i) && (i.length == 2)}
  end

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

class FalseClass
  # false is always blank
  def blank?
    true
  end
end

# Add some metaprogramming methods to avoid class << self
class Module
  # Defines an instance method within a class/module
  def class_def(name, &block)
    class_eval{define_method(name, &block)}
  end 

  private

  # Define instance method(s) that calls class method(s) of the
  # same name, caching the result in an instance variable.  Define
  # standard attr_writer method for modifying that instance variable
  def class_attr_overridable(*meths)
    meths.each{|meth| class_eval("def #{meth}; !defined?(@#{meth}) ? (@#{meth} = self.class.#{meth}) : @#{meth} end")}
    attr_writer(*meths) 
    public(*meths) 
    public(*meths.collect{|m|"#{m}="}) 
  end

  # Define instance method(s) that calls class method(s) of the
  # same name. Replaces the construct:
  #
  #   define_method(meth){self.class.send(meth)}
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
    meta_eval{alias_method to, from}
  end
  
  # Make a singleton/class attribute accessor method(s).
  # Replaces the construct:
  #
  #   class << self
  #     attr_accessor *meths
  #   end
  def metaattr_accessor(*meths)
    meta_eval{attr_accessor(*meths)}
  end

  # Make a singleton/class method(s) private.
  # Make a singleton/class attribute reader method(s).
  # Replaces the construct:
  #
  #   class << self
  #     attr_reader *meths
  #   end
  def metaattr_reader(*meths)
    meta_eval{attr_reader(*meths)}
  end
end

# Helpers from Metaid and a bit more
class Object
  # Objects are blank if they respond true to empty?
  def blank?
    respond_to?(:empty?) && empty?
  end

  # Returns true if the object is an instance of one of the classes
  def is_one_of?(*classes)
    !!classes.find{|c| is_a?(c)}
  end

  # Add methods to the object's metaclass
  def meta_def(name, &block)
    meta_eval{define_method(name, &block)}
  end 

  # Evaluate the block in the context of the object's metaclass
  def meta_eval(&block)
    metaclass.instance_eval(&block)
  end 

  # The hidden singleton lurks behind everyone
  def metaclass
    class << self
      self
    end 
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

class Range
  # Returns the interval between the beginning and end of the range.
  #
  # For exclusive ranges, is one less than the inclusive range:
  #
  #   (0..10).interval # => 10
  #   (0...10).interval # => 9
  #
  # Only works for numeric ranges, for other ranges the result is undefined,
  # and the method may raise an error.
  def interval
    last - first - (exclude_end? ? 1 : 0)
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
