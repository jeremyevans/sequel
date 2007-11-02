# Enumerable extensions.
module Enumerable
  # Invokes the specified method for each item, along with the supplied
  # arguments.
  def send_each(sym, *args)
    each {|i| i.send(sym, *args)}
  end
end

# Array extensions
class Array
  # Concatenates an array of strings into an SQL string. ANSI SQL and C-style
  # comments are removed, as well as excessive white-space.
  def to_sql
    map {|l| (l =~ /^(.*)--/ ? $1 : l).chomp}.join(' '). \
      gsub(/\/\*.*\*\//, '').gsub(/\s+/, ' ').strip
  end
end

# Range extensions
class Range
  # Returns the interval between the beginning and end of the range.
  def interval
    last - first
  end
end

module Sequel
  # LiteralString is used to represent literal SQL expressions. An 
  # LiteralString is copied verbatim into an SQL statement. Instances of
  # LiteralString can be created by calling String#expr.
  class LiteralString < ::String
  end
end

# String extensions
class String
  # Converts a string into an SQL string by removing comments.
  # See also Array#to_sql.
  def to_sql
    split($/).to_sql
  end
  
  # Splits a string into separate SQL statements, removing comments
  # and excessive white-space.
  def split_sql
    to_sql.split(';').map {|s| s.strip}
  end

  # Converts a string into an LiteralString, in order to override string
  # literalization, e.g.:
  #
  #   DB[:items].filter(:abc => 'def').sql #=>
  #     "SELECT * FROM items WHERE (abc = 'def')"
  #
  #   DB[:items].filter(:abc => 'def'.lit).sql #=>
  #     "SELECT * FROM items WHERE (abc = def)"
  #
  def lit
    Sequel::LiteralString.new(self)
  end
  
  alias_method :expr, :lit
  
  # Converts a string into a Time object.
  def to_time
    Time.parse(self)
  end
  
  # Converts a string into a column name.
  def to_field_name
    self
  end
  alias_method :to_column_name, :to_field_name
end

# Methods to format column names and associated constructs. This module is
# included in String and Symbol.
module ColumnCompositionMethods
  # Constructs a DESC clause for use in an ORDER BY clause.
  def DESC
    "#{to_column_name} DESC".lit
  end
  
  # Constructs an AS clause for column aliasing.
  def AS(target)
    "#{to_column_name} AS #{target}".lit
  end

  # Constructs a qualified wildcard (*) clause.
  def ALL
    "#{to_s}.*".lit
  end
  
  COLUMN_TITLE_RE1 = /^(.*)\sAS\s(.+)$/i.freeze
  COLUMN_TITLE_RE2 = /^([^\.]+)\.([^\.]+)$/.freeze
  
  # Returns the column name. If the column name is aliased, the alias is 
  # returned.
  def field_title
    case s = to_column_name
    when COLUMN_TITLE_RE1, COLUMN_TITLE_RE2: $2
    else
      s
    end
  end
  alias_method :column_title, :field_title
end

# String extensions
class String
  include ColumnCompositionMethods
end

# Symbol extensions
class Symbol
  include ColumnCompositionMethods
  

  COLUMN_REF_RE1 = /^(\w+)__(\w+)___(\w+)/.freeze
  COLUMN_REF_RE2 = /^(\w+)___(\w+)$/.freeze
  COLUMN_REF_RE3 = /^(\w+)__(\w+)$/.freeze
  
  # Converts a symbol into a column name. This method supports underscore
  # notation in order to express qualified (two underscores) and aliased 
  # (three underscores) columns:
  #
  #   :abc.to_column_name #=> "abc"
  #   :abc___a.to_column_name #=> "abc AS a"
  #   :items__abc.to_column_name #=> "items.abc"
  #   :items__abc___a.to_column_name #=> "items.abc AS a"
  #
  def to_field_name
    s = to_s
    case s
    when COLUMN_REF_RE1: "#{$1}.#{$2} AS #{$3}"
    when COLUMN_REF_RE2: "#{$1} AS #{$2}"
    when COLUMN_REF_RE3: "#{$1}.#{$2}"
    else
      s
    end
  end
  alias_method :to_column_name, :to_field_name
  
  # Converts missing method calls into functions on columns, if the
  # method name is made of all upper case letters.
  def method_missing(sym)
    ((s = sym.to_s) =~ /^([A-Z]+)$/) ? \
      "#{s.downcase}(#{to_column_name})".lit : super
  end
  
  # Formats an SQL function with optional parameters
  def [](*args)
    "#{to_s}(#{args.join(', ')})".lit
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
    alias_method :until, :ago

    # Returns the time at now + self.
    def from_now(t = Time.now); t + self; end
    alias_method :since, :from_now
    
    # Extends the Numeric class with numeric extensions.
    def self.use
      Numeric.send(:include, self)
    end
  end
end
