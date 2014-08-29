Sequel::JDBC.load_driver('com.ibm.db2.jcc.DB2Driver')
Sequel.require 'adapters/shared/db2'
Sequel.require 'adapters/jdbc/transactions'

module Sequel
  module JDBC
    Sequel.synchronize do
      DATABASE_SETUP[:db2] = proc do |db|
        db.extend(Sequel::JDBC::DB2::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::DB2::Dataset
        com.ibm.db2.jcc.DB2Driver
      end
    end

    class TypeConvertor
      def DB2Clob(r, i)
        if v = r.getClob(i)
          v = v.getSubString(1, v.length)
          v = Sequel::SQL::Blob.new(v) if ::Sequel::DB2::use_clob_as_blob
          v
        end
      end
    end

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
            if ::Sequel::DB2.use_clob_as_blob
              cps.setString(i, arg)
            else
              super
            end
          else
            super
          end
        end
        
        def last_insert_id(conn, opts=OPTS)
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

        def setup_type_convertor_map
          super
          map = @type_convertor_map
          types = Java::JavaSQL::Types
          map[types::NCLOB] = map[types::CLOB] = TypeConvertor::INSTANCE.method(:DB2Clob)
        end
      end

      class Dataset < JDBC::Dataset
        include Sequel::DB2::DatasetMethods
      end
    end
  end
end
