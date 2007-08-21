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
  # ExpressionString is used to represent literal SQL expressions. An 
  # ExpressionString is copied verbatim into an SQL statement. Instances of
  # ExpressionString can be created by calling String#expr.
  class ExpressionString < ::String
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

  # Converts a string into an ExpressionString, in order to override string
  # literalization, e.g.:
  #
  #   DB[:items].filter(:abc => 'def').sql #=>
  #     "SELECT * FROM items WHERE (abc = 'def')"
  #
  #   DB[:items].filter(:abc => 'def'.expr).sql #=>
  #     "SELECT * FROM items WHERE (abc = def)"
  #
  def expr
    Sequel::ExpressionString.new(self)
  end
  
  # Converts a string into a Time object.
  def to_time
    Time.parse(self)
  end
  
  # Converts a string into a field name.
  def to_field_name
    self
  end
end

# Methods to format field names and associated constructs. This module is
# included in String and Symbol.
module FieldCompositionMethods
  # Constructs a DESC clause for use in an ORDER BY clause.
  def DESC
    "#{to_field_name} DESC"
  end
  
  # Constructs an AS clause for field aliasing.
  def AS(target)
    "#{to_field_name} AS #{target}"
  end

  # Constructs a qualified wildcard (*) clause.
  def ALL
    "#{to_s}.*"
  end
  
  FIELD_TITLE_RE1 = /^(.*)\sAS\s(.+)$/i.freeze
  FIELD_TITLE_RE2 = /^([^\.]+)\.([^\.]+)$/.freeze
  
  # Returns the field name. If the field name is aliased, the alias is 
  # returned.
  def field_title
    case s = to_field_name
    when FIELD_TITLE_RE1, FIELD_TITLE_RE2: $2
    else
      s
    end
  end

  # Formats a min(x) expression.
  def MIN; "min(#{to_field_name})"; end

  # Formats a max(x) expression.
  def MAX; "max(#{to_field_name})"; end

  # Formats a sum(x) expression.
  def SUM; "sum(#{to_field_name})"; end

  # Formats an avg(x) expression.
  def AVG; "avg(#{to_field_name})"; end
end

class String
  include FieldCompositionMethods
end

# Symbol extensions
class Symbol
  include FieldCompositionMethods
  

  FIELD_REF_RE1 = /^([a-z_]+)__([a-z_]+)___([a-z_]+)/.freeze
  FIELD_REF_RE2 = /^([a-z_]+)___([a-z_]+)$/.freeze
  FIELD_REF_RE3 = /^([a-z_]+)__([a-z_]+)$/.freeze
  
  # Converts a symbol into a field name. This method supports underscore
  # notation in order to express qualified (two underscores) and aliased 
  # (three underscores) fields:
  #
  #   :abc.to_field_name #=> "abc"
  #   :abc___a.to_field_name #=> "abc AS a"
  #   :items__abc.to_field_name #=> "items.abc"
  #   :items__abc___a.to_field_name #=> "items.abc AS a"
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
end


