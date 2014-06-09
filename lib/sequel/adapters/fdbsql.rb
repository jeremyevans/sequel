#
# FoundationDB SQL Layer Sequel Adapter
# Copyright (c) 2013-2014 FoundationDB, LLC
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#


require 'sequel/adapters/fdbsql/connection'
require 'sequel/adapters/utils/pg_types'

module Sequel
  module Fdbsql

    class Database < Sequel::Database

      set_adapter_scheme :fdbsql

      def connect(server)
        opts = server_opts(server)
        Connection.new(apply_default_options(opts))
      end

      def execute(sql, opts = {}, &block)
        res = nil
        synchronize(opts[:server]) do |conn|
            res = log_yield(sql) { conn.query(sql) }
            yield res if block_given?
            res.cmd_tuples
        end
      end

      # the sql layer supports DROP TABLE IF EXISTS
      def supports_drop_table_if_exists?
        true
      end

      def supports_schema_parsing?
        true
      end

      # Like PostgreSQL fdbsql folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers on input.
      def identifier_input_method_default
        nil
      end

      # Like PostgreSQL fdbsql folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers on output.
      def identifier_output_method_default
        nil
      end

      # like PostgreSQL fdbsql uses SERIAL psuedo-type instead of AUTOINCREMENT for
      # managing incrementing primary keys.
      def serial_primary_key_options
        {:primary_key => true, :serial => true, :type=>Integer}
      end

      # Handle bigserial type if :serial option is present
      def type_literal_generic_bignum(column)
        # TODO bigserial or BGSERIAL, the docs say bgserial, but that seems wrong
        column[:serial] ? :bigserial : super
      end

      # Handle serial type if :serial option is present
      def type_literal_generic_integer(column)
        column[:serial] ? :serial : super
      end


      def schema_parse_table(table_name, options = {})
        schema = options[:schema]

        # Use literal here to wrap in quotes, still need to figure out casing
        dataset = metadata_dataset.with_sql(
                                            'SELECT column_name, is_nullable AS allow_null, column_default AS "default", data_type AS db_type ' +
                                            'FROM information_schema.columns ' +
                                            "WHERE table_name = #{literal(table_name)} " +
                                            (schema ? " AND table_schema = #{literal(schema)} " : ''))

        dataset.map do |row|
          row[:default] = nil if blank_object?(row[:default])
          row[:type] = schema_column_type(row[:db_type])
          [row.delete(:column_name).to_sym, row]
        end
      end


      private


      CONNECTION_DEFAULTS = {
        :host => 'localhost',
        :port => 15432,
        :username => 'fdbsql',
        :password => '',
      }

      def apply_default_options(sequel_options)
        config = CONNECTION_DEFAULTS.merge(sequel_options)
        config[:encoding] =
          config[:charset] || 'UTF8'    unless config[:encoding]

        if config.key?(:database)
          database = config[:database]
        else
          raise ArgumentError, "No database specified. Missing config option: database"
        end

        return config
      end

    end


    # TODO put in own file if this becomes more than 5 lines
    class Dataset < Sequel::Dataset
      Sequel::Fdbsql::Database::DatasetClass = self

      def columns
        # Note: @columns is used for Sequel::Dataset for something else, so we have to
        # have a different name here
        return @column_names if @column_names
        ds = unfiltered.unordered.clone(:distinct => nil, :limit => 0, :offset => nil)
        @db.execute(ds.select_sql) {|res| set_columns(res) }
        @column_names
      end

      def fetch_rows(sql)
        execute(sql) do |res|
          columns = set_columns(res)
          yield_hash_rows(res, columns) {|h| yield h}
        end
      end

      Dataset.def_sql_method(self, :insert, %w'with insert into columns values returning')

      # Returning is always supported.
      def supports_returning?(type)
        true
      end

      # Insert a record returning the record inserted.  Always returns nil without
      # inserting a query if disable_insert_returning is used.
      def insert_select(*values)
        returning.insert(*values){|r| return r} unless @opts[:disable_insert_returning]
      end

      private


      # For each row in the result set, yield a hash with column name symbol
      # keys and typecasted values.
      def yield_hash_rows(res, cols)
        res.ntuples.times do |recnum|
          converted_rec = {}
          cols.each do |fieldnum, type_proc, fieldsym|
            value = res.getvalue(recnum, fieldnum)
            converted_rec[fieldsym] = (value && type_proc) ? type_proc.call(value) : value
          end
          yield converted_rec
        end
      end


      def set_columns(res)
        cols = []
        procs = Sequel::Postgres::PG_TYPES
        res.nfields.times do |fieldnum|
          cols << [fieldnum, procs[res.ftype(fieldnum)], output_identifier(res.fname(fieldnum))]
        end
        @column_names = cols.map{|c| c[2]}
        cols
      end
    end
  end
end
