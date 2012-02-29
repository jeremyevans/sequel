# The pg_from_only extension adds the pg_only method to any identifier variant
# (Symbol, SQL::Identifier, SQL::QualifiedIdentifier), which allows to compose
# PostgreSQL `select * from ONLY table` expression
#
#   db[:table.pg_only].sql             # select * from only table
#   db[:table.pg_only].update(:v => 1) # update only table set v = 1
#   db[:table.pg_only].delete          # delete from only table

module Sequel
  module SQL
    # Includes a +pg_only+ method that created <tt>OnlyIdentifier</tt>s, used for selection from ONLY in PostgreSQL
    module PgOnlyMethods
      # Add to a reciever prefix ONLY for using in PostgreSQL inheritance
      #
      #   :table.pg_only  # ONLY "table"
      #   :table.qualify(:schema).pg_only  # ONLY "schema"."table"
      def pg_only
        PgOnlyIdentifier.new(self)
      end
    end

    class Identifier
      include PgOnlyMethods
    end

    class QualifiedIdentifier < GenericExpression
      include PgOnlyMethods
    end

    # Represents a selection ONLY from table (PostgreSQL inheritance)
    class PgOnlyIdentifier < GenericExpression
      # Reference to a table
      attr_reader :table

      def initialize(table)
        @table = table
      end

      to_s_method :only_identifier_sql
    end
  end

  module Postgres
    module DatasetMethods
      ONLY = 'ONLY '.freeze

      # Skip ONLY when writting qualified identifier
      def qualified_identifier_sql_append(sql, qcr)
        unless SQL::PgOnlyIdentifier === qcr.table
          super
        else
          super(sql, SQL::QualifiedIdentifier.new(qcr.table.table, qcr.column))
        end
      end

      # SQL fragment for selection from ONLY table in PostgreSQL
      def only_identifier_sql_append(sql, only)
        sql << ONLY
        case t = only.table
        when Symbol, SQL::QualifiedIdentifier, SQL::Identifier
          literal_append(sql, t)
        else
          quote_identifier_append(sql, t)
        end
      end
    end
  end

end

class Symbol
  include Sequel::SQL::PgOnlyMethods
end
