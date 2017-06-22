# frozen-string-literal: true

Sequel::JDBC.load_driver('com.ibm.db2.jcc.DB2Driver')
Sequel.require 'adapters/shared/db2'
Sequel.require 'adapters/jdbc/transactions'

module Sequel
  module JDBC
    Sequel.synchronize do
      DATABASE_SETUP[:db2] = proc do |db|
        (class << db; self; end).class_eval do
          alias jdbc_schema_parse_table schema_parse_table
          alias jdbc_tables tables
          alias jdbc_views views
          alias jdbc_indexes indexes
        end
        db.extend(Sequel::JDBC::DB2::DatabaseMethods)
        (class << db; self; end).class_eval do
          alias schema_parse_table jdbc_schema_parse_table
          alias tables jdbc_tables
          alias views jdbc_views
          alias indexes jdbc_indexes
          %w'schema_parse_table tables views indexes'.each do |s|
            class_eval(<<-END, __FILE__, __LINE__+1)
              def jdbc_#{s}(*a)
                Sequel::Deprecation.deprecate("Database#jdbc_#{s} in the jdbc/db2 adapter", "Use Database\##{s} instead")
                #{s}(*a)
              end
            END
            # remove_method(:"jdbc_#{s}") # SEQUEL5
          end
        end
        db.dataset_class = Sequel::JDBC::DB2::Dataset
        com.ibm.db2.jcc.DB2Driver
      end
    end

    # SEQUEL5: Remove
    class Type_Convertor
      def DB2Clob(r, i)
        if v = r.getClob(i)
          v = v.getSubString(1, v.length)
          v = Sequel::SQL::Blob.new(v) if ::Sequel::DB2::use_clob_as_blob
          v
        end
      end
    end

    # Database and Dataset instance methods for DB2 specific
    # support via JDBC.
    module DB2
      # Database instance methods for DB2 databases accessed via JDBC.
      module DatabaseMethods
        include Sequel::DB2::DatabaseMethods
        include Sequel::JDBC::Transactions

        PRIMARY_KEY_INDEX_RE = /\Asql\d+\z/i.freeze
        Sequel::Deprecation.deprecate_constant(self, :PRIMARY_KEY_INDEX_RE)
        IDENTITY_VAL_LOCAL = "SELECT IDENTITY_VAL_LOCAL() FROM SYSIBM.SYSDUMMY1".freeze
        Sequel::Deprecation.deprecate_constant(self, :IDENTITY_VAL_LOCAL)
        
        private

        def set_ps_arg(cps, arg, i)
          case arg
          when Sequel::SQL::Blob
            if use_clob_as_blob
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
            sql = "SELECT IDENTITY_VAL_LOCAL() FROM SYSIBM.SYSDUMMY1"
            rs = log_connection_yield(sql, conn){stmt.executeQuery(sql)}
            rs.next
            rs.getLong(1)
          end
        end
        
        # Primary key indexes appear to be named sqlNNNN on DB2
        def primary_key_index_re
          /\Asql\d+\z/i
        end

        def setup_type_convertor_map
          super
          map = @type_convertor_map
          types = Java::JavaSQL::Types
          map[types::NCLOB] = map[types::CLOB] = method(:convert_clob)
        end

        def convert_clob(r, i)
          if v = r.getClob(i)
            v = v.getSubString(1, v.length)
            v = Sequel::SQL::Blob.new(v) if use_clob_as_blob
            v
          end
        end
      end

      class Dataset < JDBC::Dataset
        include Sequel::DB2::DatasetMethods
      end
    end
  end
end
