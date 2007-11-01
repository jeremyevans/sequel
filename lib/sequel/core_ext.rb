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
module FieldCompositionMethods
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
  
  FIELD_TITLE_RE1 = /^(.*)\sAS\s(.+)$/i.freeze
  FIELD_TITLE_RE2 = /^([^\.]+)\.([^\.]+)$/.freeze
  
  # Returns the column name. If the column name is aliased, the alias is 
  # returned.
  def field_title
    case s = to_column_name
    when FIELD_TITLE_RE1, FIELD_TITLE_RE2: $2
    else
      s
    end
  end
  alias_method :column_title, :field_title
end

# String extensions
class String
  include FieldCompositionMethods
end

# Symbol extensions
class Symbol
  include FieldCompositionMethods
  

  FIELD_REF_RE1 = /^(\w+)__(\w+)___(\w+)/.freeze
  FIELD_REF_RE2 = /^(\w+)___(\w+)$/.freeze
  FIELD_REF_RE3 = /^(\w+)__(\w+)$/.freeze
  
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
    when FIELD_REF_RE1: "#{$1}.#{$2} AS #{$3}"
    when FIELD_REF_RE2: "#{$1} AS #{$2}"
    when FIELD_REF_RE3: "#{$1}.#{$2}"
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


