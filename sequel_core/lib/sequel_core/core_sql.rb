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
  # LiteralString can be created by calling String#lit.
  class LiteralString < ::String
  end
end

class String
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
  
  # Splits a string into separate SQL statements, removing comments
  # and excessive white-space.
  def split_sql
    to_sql.split(';').map {|s| s.strip}
  end

  # Converts a string into an SQL string by removing comments.
  # See also Array#to_sql.
  def to_sql
    split("\n").to_sql
  end
  
  # Converts a string into a Date object.
  def to_date
    begin
      Date.parse(self)
    rescue => e
      raise Sequel::Error::InvalidValue, "Invalid date value '#{self}' (#{e.message})"
    end
  end

  # Converts a string into a DateTime object.
  def to_datetime
    begin
      DateTime.parse(self)
    rescue => e
      raise Sequel::Error::InvalidValue, "Invalid date value '#{self}' (#{e.message})"
    end
  end

  # Converts a string into a Time object.
  def to_time
    begin
      Time.parse(self)
    rescue => e
      raise Sequel::Error::InvalidValue, "Invalid time value '#{self}' (#{e.message})"
    end
  end
end


module Sequel
  module SQL
    module ColumnMethods
      AS = 'AS'.freeze
      DESC = 'DESC'.freeze
      ASC = 'ASC'.freeze
      
      def as(a); ColumnExpr.new(self, AS, a); end
      
      def desc; ColumnExpr.new(self, DESC); end
      
      def asc; ColumnExpr.new(self, ASC); end

      def cast_as(t)
        t = t.to_s.lit if t.is_a?(Symbol)
        Sequel::SQL::Function.new(:cast, self.as(t))
      end
    end

    class Expression
      include ColumnMethods

      def lit; self; end
    end
    
    class ColumnExpr < Expression
      attr_reader :l, :op, :r

      def initialize(l, op, r = nil)
        @l, @op, @r = l, op, r
      end
      
      def to_s(ds)
        ds.column_expr_sql(self)
      end
    end
    
    class QualifiedColumnRef < Expression
      attr_reader :table, :column

      def initialize(table, column)
        @table, @column = table, column
      end
      
      def to_s(ds)
        ds.qualified_column_ref_sql(self)
      end 
    end
    
    class Function < Expression
      attr_reader :f, :args

      def initialize(f, *args)
        @f, @args = f, args
      end

      # Functions are considered equivalent if they
      # have the same class, function, and arguments.
      def ==(x)
         x.class == self.class && @f == x.f && @args == x.args
      end

      def to_s(ds)
        ds.function_sql(self)
      end
    end
    
    class Subscript < Expression
      attr_reader :f, :sub

      def initialize(f, sub)
        @f, @sub = f, sub
      end

      def |(sub)
        Subscript.new(@f, @sub << Array(sub))
      end
      
      def to_s(ds)
        ds.subscript_sql(self)
      end
    end
    
    class ColumnAll < Expression
      attr_reader :table

      def initialize(table)
        @table = table
      end

      # ColumnAll expressions are considered equivalent if they
      # have the same class and string representation
      def ==(x)
        x.class == self.class && @table == x.table
      end

      def to_s(ds)
        ds.column_all_sql(self)
      end
    end
  end
end

class String
  include Sequel::SQL::ColumnMethods
end

class Symbol
  include Sequel::SQL::ColumnMethods

  def *
    Sequel::SQL::ColumnAll.new(self);
  end

  def [](*args)
    Sequel::SQL::Function.new(self, *args)
  end

  def |(sub)
    Sequel::SQL::Subscript.new(self, Array(sub))
  end
  
  def to_column_ref(ds)
    ds.symbol_to_column_ref(self)
  end
end
