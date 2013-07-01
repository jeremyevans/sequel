module Sequel
  class Dataset
    module Replace
      INSERT = Dataset::INSERT
      REPLACE = 'REPLACE'.freeze

      # Execute a REPLACE statement on the database.
      def replace(*values)
        execute_insert(replace_sql(*values))
      end

      # SQLite specific syntax for REPLACE (aka UPSERT, or update if exists,
      # insert if it doesn't).
      def replace_sql(*values)
        clone(:replace=>true).insert_sql(*values)
      end

      # Replace multiple rows in a single query.
      def multi_replace(*values)
        clone(:replace=>true).multi_insert(*values)
      end

      # Whether the database supports REPLACE syntax
      def supports_replace?
        true
      end

      private

      # If this is an replace instead of an insert, use replace instead
      def insert_insert_sql(sql)
        sql << (@opts[:replace] ? REPLACE : INSERT)
      end
    end
  end
end
