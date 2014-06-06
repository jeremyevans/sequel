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
            res
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
        return @columns if @columns
        ds = unfiltered.unordered.clone(:distinct => nil, :limit => 0, :offset => nil)
        res = @db.execute(ds.select_sql)
        set_columns(res)
      end

      def fetch_rows(sql)
        execute(sql) do |res|
          set_columns(res) unless @columns
          res.each do |row|
            yield symbolize_keys(row)
          end
        end
      end

      private

      def symbolize_keys(hash)
        result = {}
        hash.each_pair do |key, value|
          begin
            result[key.to_sym] = value
          rescue
            result[key] = value
          end
        end
        result
      end

      def set_columns(fetch_result)
        @columns = fetch_result.fields.map do |column|
          output_identifier(column)
        end
      end
    end
  end
end
