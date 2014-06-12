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

require 'sequel/adapters/utils/pg_types'
module Sequel
  module Fdbsql

    class Dataset < Sequel::Dataset

      def fetch_rows(sql)
        execute(sql) do |res|
          columns = set_columns(res)
          yield_hash_rows(res, columns) {|h| yield h}
        end
      end

      Dataset.def_sql_method(self, :delete, %w'with delete from using where returning')
      Dataset.def_sql_method(self, :insert, %w'with insert into columns values returning')
      Dataset.def_sql_method(self, :update, %w'with update table set from where returning')

      # Returning is always supported.
      def supports_returning?(type)
        true
      end

      # Insert given values into the database.
      def insert(*values)
        if @opts[:returning]
          # Already know which columns to return, let the standard code handle it
          super
        elsif @opts[:sql] || @opts[:disable_insert_returning]
          # Raw SQL used or RETURNING disabled, just use the default behavior
          # and return nil since sequence is not known.
          super
          nil
        else
          # Force the use of RETURNING with the primary key value,
          # unless it has been disabled.
          returning(insert_pk).insert(*values){|r| return r.values.first}
        end
      end

      # Insert a record returning the record inserted.  Always returns nil without
      # inserting a query if disable_insert_returning is used.
      def insert_select(*values)
        returning.insert(*values){|r| return r} unless @opts[:disable_insert_returning]
      end

      # For multiple table support, PostgreSQL requires at least
      # two from tables, with joins allowed.
      def join_from_sql(type, sql)
        if(from = @opts[:from][1..-1]).empty?
          raise(Error, 'Need multiple FROM tables if updating/deleting a dataset with JOINs') if @opts[:join]
        else
          sql << SPACE << type.to_s << SPACE
          source_list_append(sql, from)
          select_join_sql(sql)
        end
      end

      # Use FROM to specify additional tables in an update query
      def update_from_sql(sql)
        join_from_sql(:FROM, sql)
      end

      # Use USING to specify additional tables in a delete query
      def delete_using_sql(sql)
        join_from_sql(:USING, sql)
      end

      # fdbsql does not support FOR UPDATE, because it's unnecessary with the transaction model
      def select_lock_sql(sql)
        @opts[:lock] == :update ? sql : super
      end

      # Emulate the case insensitive LIKE operator and the bitwise operators.
      def complex_expression_sql_append(sql, op, args)
        case op
        when :&, :|, :^, :<<, :>>, :'B~'
          complex_expression_emulate_append(sql, op, args)
        else
          super
        end
      end

      # FDBSQL uses a preceding x for hex escaping strings
      def literal_blob_append(sql, v)
        if v.empty?
          sql << "''"
        else
          sql << "x'#{v.unpack('H*').first}'"
        end
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

      # Postgres supports named types in the db, if we want to support anything that's not built in, this
      # will have to be changed to not be a constant
      CONVERSION_PROCS = Sequel::Postgres::PG_TYPES.dup
      CONVERSION_PROCS[16] = Proc.new {|s| s == 'true'}
      CONVERSION_PROCS.freeze

      def set_columns(res)
        cols = []
        procs = CONVERSION_PROCS
        res.nfields.times do |fieldnum|
          cols << [fieldnum, procs[res.ftype(fieldnum)], output_identifier(res.fname(fieldnum))]
        end
        @columns = cols.map{|c| c[2]}
        cols
      end

      # Return the primary key to use for RETURNING in an INSERT statement
      def insert_pk
        if (f = opts[:from]) && !f.empty?
          case t = f.first
          when Symbol, String, SQL::Identifier, SQL::QualifiedIdentifier
            if pk = db.primary_key(t)
              Sequel::SQL::Identifier.new(pk)
            end
          end
        end
      end

    end
  end
end
