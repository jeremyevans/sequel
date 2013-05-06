Sequel.require 'adapters/shared/db2'
Sequel.require 'adapters/jdbc/transactions'

module Sequel
  module JDBC
    class Database
      # Alias the generic JDBC versions so they can be called directly later
      alias jdbc_schema_parse_table schema_parse_table
      alias jdbc_tables tables
      alias jdbc_views views
      alias jdbc_indexes indexes
    end
    
    # Database and Dataset instance methods for DB2 specific
    # support via JDBC.
    module DB2
      # Database instance methods for DB2 databases accessed via JDBC.
      module DatabaseMethods
        extend Sequel::Database::ResetIdentifierMangling
        PRIMARY_KEY_INDEX_RE = /\Asql\d+\z/i.freeze

        include Sequel::DB2::DatabaseMethods
        include Sequel::JDBC::Transactions
        IDENTITY_VAL_LOCAL = "SELECT IDENTITY_VAL_LOCAL() FROM SYSIBM.SYSDUMMY1".freeze
        
        %w'schema_parse_table tables views indexes'.each do |s|
          class_eval("def #{s}(*a) jdbc_#{s}(*a) end", __FILE__, __LINE__)
        end

        private

        def set_ps_arg(cps, arg, i)
          case arg
          when Sequel::SQL::Blob
            cps.setString(i, arg)
          else
            super
          end
        end
        
        def last_insert_id(conn, opts={})
          statement(conn) do |stmt|
            sql = IDENTITY_VAL_LOCAL
            rs = log_yield(sql){stmt.executeQuery(sql)}
            rs.next
            rs.getInt(1)
          end
        end
        
        # Primary key indexes appear to be named sqlNNNN on DB2
        def primary_key_index_re
          PRIMARY_KEY_INDEX_RE
        end
      end

      class Dataset < JDBC::Dataset
        include Sequel::DB2::DatasetMethods

        class ::Sequel::JDBC::Dataset::TYPE_TRANSLATOR
          def db2_clob(v) Sequel::SQL::Blob.new(v.getSubString(1, v.length)) end
        end

        DB2_CLOB_METHOD = TYPE_TRANSLATOR_INSTANCE.method(:db2_clob)
      
        private

        # Return clob as blob if use_clob_as_blob is true
        def convert_type_proc(v)
          case v
          when JAVA_SQL_CLOB
            ::Sequel::DB2::use_clob_as_blob ? DB2_CLOB_METHOD : super
          else
            super
          end
        end
      end
    end
  end
end
