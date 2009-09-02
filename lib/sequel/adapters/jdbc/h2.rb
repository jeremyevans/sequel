module Sequel
  module JDBC
    # Database and Dataset support for H2 databases accessed via JDBC.
    module H2
      # Instance methods for H2 Database objects accessed via JDBC.
      module DatabaseMethods
        PRIMARY_KEY_INDEX_RE = /\Aprimary_key/i.freeze
      
        # H2 uses the :h2 database type.
        def database_type
          :h2
        end

        # Return Sequel::JDBC::H2::Dataset object with the given opts.
        def dataset(opts=nil)
          Sequel::JDBC::H2::Dataset.new(self, opts)
        end
        
        # H2 uses an IDENTITY type
        def serial_primary_key_options
          {:primary_key => true, :type => :identity}
        end
        
        private
        
        # H2 needs to add a primary key column as a constraint
        def alter_table_sql(table, op)
          case op[:op]
          when :add_column
            if (pk = op.delete(:primary_key)) || (ref = op.delete(:table))
              sqls = [super(table, op)]
              sqls << "ALTER TABLE #{quote_schema_table(table)} ADD PRIMARY KEY (#{quote_identifier(op[:name])})" if pk
              if ref
                op[:table] = ref
                sqls << "ALTER TABLE #{quote_schema_table(table)} ADD FOREIGN KEY (#{quote_identifier(op[:name])}) #{column_references_sql(op)}"
              end
              sqls
            else
              super(table, op)
            end
          when :rename_column
            "ALTER TABLE #{quote_schema_table(table)} ALTER COLUMN #{quote_identifier(op[:name])} RENAME TO #{quote_identifier(op[:new_name])}"
          when :set_column_null
            "ALTER TABLE #{quote_schema_table(table)} ALTER COLUMN #{quote_identifier(op[:name])} SET#{' NOT' unless op[:null]} NULL"
          when :set_column_type
            "ALTER TABLE #{quote_schema_table(table)} ALTER COLUMN #{quote_identifier(op[:name])} #{type_literal(op)}"
          else
            super(table, op)
          end
        end
        
        # Default to a single connection for a memory database.
        def connection_pool_default_options
          o = super
          uri == 'jdbc:h2:mem:' ? o.merge(:max_connections=>1) : o
        end
      
        # Use IDENTITY() to get the last inserted id.
        def last_insert_id(conn, opts={})
          stmt = conn.createStatement
          begin
            rs = stmt.executeQuery('SELECT IDENTITY();')
            rs.next
            rs.getInt(1)
          ensure
            stmt.close
          end 
        end
        
        def primary_key_index_re
          PRIMARY_KEY_INDEX_RE
        end
      end
      
      # Dataset class for H2 datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        SELECT_CLAUSE_METHODS = clause_methods(:select, %w'distinct columns from join where group having compounds order limit')
        
        # H2 requires SQL standard datetimes
        def requires_sql_standard_datetimes?
          true
        end

        # H2 doesn't support IS TRUE
        def supports_is_true?
          false
        end
        
        private
      
        def select_clause_methods
          SELECT_CLAUSE_METHODS
        end
      end
    end
  end
end
