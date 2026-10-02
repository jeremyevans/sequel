# frozen-string-literal: true
#
# The pg_loose_count extension looks at the table statistics
# in the PostgreSQL system tables to get a fast approximate
# count of the number of rows in a given table:
#
#   DB.loose_count(:table) # => 123456
#
# It can also support schema qualified tables:
#
#   DB.loose_count(Sequel[:schema][:table]) # => 123456
#
# To get the loose count of all tables in a given schema:
#
#   DB.loose_counts(:schema) # => {table: 123456, ...}
#
# How accurate these counts are depends on the number of rows
# added/deleted from the table since the last time it was
# analyzed. If the table has not been vacuumed or analyzed
# yet, this can return 0 or -1 depending on the PostgreSQL
# version in use.
# 
# To load the extension into the database:
#
#   DB.extension :pg_loose_count
#
# Related module: Sequel::Postgres::LooseCount

#
module Sequel
  module Postgres
    module LooseCount
      # Look at the table statistics for the given table to get
      # an approximate count of the number of rows.
      def loose_count(table)
        from(:pg_class).where(:oid=>regclass_oid(table)).get(Sequel.cast(:reltuples, Integer))
      end

      # Look at the table statistics for the given schema and return a
      # hash with table name keys and approximate counts. Results include
      # normal tables, materialized views, and partitioned tables.
      def loose_counts(schema)
        case schema
        when Symbol
          schema = schema.to_s
        when String
          nil
        when SQL::Identifier
          schema = schema.value.to_s
        else
          raise Error, "unsupported argument type, should be symbol, string, or Sequel::SQL::Identifier, given #{schema.inspect}"
        end

        namespace_oid = if server_version >= 90500
          Sequel.function(:to_regnamespace, schema).cast(:oid)
        else
          from(:pg_namespace).where(nspname: schema).select(:oid)
        end

        hash = {}
        from(:pg_class).
          where(relnamespace: namespace_oid, relkind: %w"r m p").
          order(:relname).
          select_map([:relname, Sequel.cast(:reltuples, Integer).as(:v)]).
          each do |k, v|
            hash[k.to_sym] = v
          end
        hash
      end
    end
  end

  Database.register_extension(:pg_loose_count, Postgres::LooseCount)
end
