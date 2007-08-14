# Enumerable extensions.
module Enumerable
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
  
  # Convert a string into an Expression String
  def expr
    Sequel::ExpressionString.new(self)
  end
  
  # Convert a string into a Time object
  def to_time
    Time.parse(self)
  end
end

# Symbol extensions
class Symbol
  def DESC
    "#{to_field_name} DESC"
  end
  
  def AS(target)
    "#{to_field_name} AS #{target}"
  end

  FIELD_REF_RE1 = /^([a-z_]+)__([a-z_]+)___([a-z_]+)/.freeze
  FIELD_REF_RE2 = /^([a-z_]+)___([a-z_]+)$/.freeze
  FIELD_REF_RE3 = /^([a-z_]+)__([a-z_]+)$/.freeze
  DOUBLE_UNDERSCORE = '__'.freeze
  PERIOD = '.'.freeze
  
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
  
  def ALL
    "#{to_s}.*"
  end

  def MIN; "min(#{to_field_name})"; end
  def MAX; "max(#{to_field_name})"; end
  def SUM; "sum(#{to_field_name})"; end
  def AVG; "avg(#{to_field_name})"; end
end
