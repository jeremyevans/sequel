# frozen-string-literal: true
#
# The pg_schema_caching extension builds on top of the schema_caching
# extension, and allows it to handle custom PostgreSQL types. On
# PostgreSQL, column schema hashes include an :oid entry for the OID
# for the column's type.  For custom types, this OID is dependent on
# the PostgreSQL database, so in most cases, test and development
# versions of the same database, created with the same migrations,
# will have different OIDs.
#
# To fix this case, the pg_schema_caching extension removes custom
# OIDs from the schema cache when dumping the schema, replacing them
# with a placeholder. When loading the cached schema, the Database
# object makes a single query to get the OIDs for all custom types
# used by the cached schema, and it updates all related column
# schema hashes to set the correct :oid entry for the current
# database.
#
# Related module: Sequel::Postgres::SchemaCaching

require_relative "schema_caching"

module Sequel
  module Postgres
    module SchemaCaching
      include Sequel::SchemaCaching

      private

      # Load custom oids from database when loading schema cache file.
      def load_schema_cache_file(file)
        set_custom_oids_for_cached_schema(super)
      end

      # Find all column schema hashes that use custom types.
      # Load the oids for custom types in a single query, and update
      # each related column schema hash with the correct oid.
      def set_custom_oids_for_cached_schema(schemas)
        custom_oid_rows = {}

        schemas.each_value do |cols|
          cols.each do |_, h|
            if h[:oid] == :custom
              (custom_oid_rows[h[:db_type]] ||= []) << h
            end
          end
        end

        unless custom_oid_rows.empty?
          from(:pg_type).where(:typname=>custom_oid_rows.keys).select_hash(:typname, :oid).each do |name, oid|
            custom_oid_rows.delete(name).each do |row|
              row[:oid] = oid
            end
          end
        end

        unless custom_oid_rows.empty?
          warn "Could not load OIDs for the following custom types: #{custom_oid_rows.keys.sort.join(", ")}", uplevel: 3

          schemas.keys.each do |k|
            if schemas[k].any?{|_,h| h[:oid] == :custom}
              # Remove schema entry for table, so it will be queried at runtime to get the correct oids
              schemas.delete(k)
            end
          end
        end

        schemas
      end

      # Replace :oid entries for custom types with :custom.
      def dumpable_schema_cache
        sch = super

        sch.each_value do |cols|
          cols.each do |_, h|
            if (oid = h[:oid]) && oid >= 10000
              h[:oid] = :custom
            end
          end
        end

        sch
      end
    end
  end

  Database.register_extension(:pg_schema_caching, Postgres::SchemaCaching)
end

