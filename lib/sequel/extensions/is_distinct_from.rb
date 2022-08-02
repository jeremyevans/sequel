# frozen-string-literal: true
#
# The is_distinct_from extension adds the ability to use the
# SQL standard IS DISTINCT FROM operator, which is similar to the
# not equals operator, except that NULL values are considered
# equal.  PostgreSQL, SQLite 3.39+, and H2 currently support this operator.  On 
# other databases, support is emulated.
#
# First, you need to load the extension into the database:
#
#   DB.extension :is_distinct_from
#
# Then you can use the Sequel.is_distinct_from to create the expression
# objects:  
#
#   expr = Sequel.is_distinct_from(:column_a, :column_b)
#   # (column_a IS DISTINCT FROM column_b)
#
# You can also use the +is_distinct_from+ method on most Sequel expressions:
#
#   expr = Sequel[:column_a].is_distinct_from(:column_b)
#   # (column_a IS DISTINCT FROM column_b)
#   
# These expressions can be used in your datasets, or anywhere else that
# Sequel expressions are allowed:
#
#   DB[:table].where(expr)
#
# Related module: Sequel::SQL::IsDistinctFrom

#
module Sequel
  module SQL
    module Builders
      # Return a IsDistinctFrom expression object, using the IS DISTINCT FROM operator
      # with the given left hand side and right hand side.
      def is_distinct_from(lhs, rhs)
        BooleanExpression.new(:NOOP, IsDistinctFrom.new(lhs, rhs))
      end
    end

    # Represents an SQL expression using the IS DISTINCT FROM operator.
    class IsDistinctFrom < GenericExpression
      # These methods are added to expressions, allowing them to return IS DISTINCT
      # FROM expressions based on the receiving expression.
      module Methods
        # Return a IsDistinctFrom expression, using the IS DISTINCT FROM operator,
        # with the receiver as the left hand side and the argument as the right hand side.
        def is_distinct_from(rhs)
          BooleanExpression.new(:NOOP, IsDistinctFrom.new(self, rhs))
        end
      end

      # These methods are added to datasets using the is_distinct_from extension
      # extension, for the purposes of correctly literalizing IsDistinctFrom
      # expressions for the appropriate database type.
      module DatasetMethods
        # Append the SQL fragment for the IS DISTINCT FROM expression to the SQL query.
        def is_distinct_from_sql_append(sql, idf)
          lhs = idf.lhs
          rhs = idf.rhs

          if supports_is_distinct_from?
            sql << "("
            literal_append(sql, lhs)
            sql << " IS DISTINCT FROM "
            literal_append(sql, rhs)
            sql << ")"
          elsif db.database_type == :derby && (lhs == nil || rhs == nil)
            if lhs == nil && rhs == nil
              sql << literal_false
            elsif lhs == nil
              literal_append(sql, ~Sequel.expr(rhs=>nil))
            else
              literal_append(sql, ~Sequel.expr(lhs=>nil))
            end
          else
            literal_append(sql, Sequel.case({(Sequel.expr(lhs=>rhs) | [[lhs, nil], [rhs, nil]]) => 0}, 1) => 1)
          end
        end

        private

        # Whether the database supports IS DISTINCT FROM.
        def supports_is_distinct_from?
          if defined?(super)
            return super
          end
        
          case db.database_type
          when :postgres, :h2
            true
          when :sqlite
            db.sqlite_version >= 33900
          else
            false
          end
        end
      end

      # The left hand side of the IS DISTINCT FROM expression.
      attr_reader :lhs

      # The right hand side of the IS DISTINCT FROM expression.
      attr_reader :rhs

      def initialize(lhs, rhs)
        @lhs = lhs
        @rhs = rhs
      end

      to_s_method :is_distinct_from_sql
    end
  end

  class SQL::GenericExpression
    include SQL::IsDistinctFrom::Methods
  end

  class LiteralString
    include SQL::IsDistinctFrom::Methods
  end

  Dataset.register_extension(:is_distinct_from, SQL::IsDistinctFrom::DatasetMethods)
end

# :nocov:
if Sequel.core_extensions?
  class Symbol
    include Sequel::SQL::IsDistinctFrom::Methods
  end
end

if defined?(Sequel::CoreRefinements)
  module Sequel::CoreRefinements
    refine Symbol do
      send INCLUDE_METH, Sequel::SQL::IsDistinctFrom::Methods
    end
  end
end
# :nocov:
