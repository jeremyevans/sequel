module Sequel
  module JDBC
    # Database and Dataset support for H2 databases accessed via JDBC.
    module H2
      # Instance methods for H2 Database objects accessed via JDBC.
      module DatabaseMethods
        PRIMARY_KEY_INDEX_RE = /\Aprimary_key/i.freeze
      
        # Commit an existing prepared transaction with the given transaction
        # identifier string.
        def commit_prepared_transaction(transaction_id)
          run("COMMIT TRANSACTION #{transaction_id}")
        end

        # H2 uses the :h2 database type.
        def database_type
          :h2
        end

        # Return Sequel::JDBC::H2::Dataset object with the given opts.
        def dataset(opts=nil)
          Sequel::JDBC::H2::Dataset.new(self, opts)
        end
        
        # Rollback an existing prepared transaction with the given transaction
        # identifier string.
        def rollback_prepared_transaction(transaction_id)
          run("ROLLBACK TRANSACTION #{transaction_id}")
        end

        # H2 uses an IDENTITY type
        def serial_primary_key_options
          {:primary_key => true, :type => :identity}
        end

        # H2 supports prepared transactions
        def supports_prepared_transactions?
          true
        end
        
        # H2 supports savepoints
        def supports_savepoints?
          true
        end
        
        private
        
        # If the :prepare option is given and we aren't in a savepoint,
        # prepare the transaction for a two-phase commit.
        def commit_transaction(conn, opts={})
          if opts[:prepare] && Thread.current[:sequel_transaction_depth] <= 1
            log_connection_execute(conn, "PREPARE COMMIT #{opts[:prepare]}")
          else
            super
          end
        end

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
          statement(conn) do |stmt|
            rs = stmt.executeQuery('SELECT IDENTITY();')
            rs.next
            rs.getInt(1)
          end
        end
        
        def primary_key_index_re
          PRIMARY_KEY_INDEX_RE
        end
      end
      
      # Dataset class for H2 datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        SELECT_CLAUSE_METHODS = clause_methods(:select, %w'distinct columns from join where group having compounds order limit')
        BITWISE_METHOD_MAP = {:& =>:BITAND, :| => :BITOR, :^ => :BITXOR}
        
        # Work around H2's lack of a case insensitive LIKE operator
        def complex_expression_sql(op, args)
          case op
          when :ILIKE
            super(:LIKE, [SQL::PlaceholderLiteralString.new("CAST(? AS VARCHAR_IGNORECASE)", [args.at(0)]), args.at(1)])
          when :"NOT ILIKE"
            super(:"NOT LIKE", [SQL::PlaceholderLiteralString.new("CAST(? AS VARCHAR_IGNORECASE)", [args.at(0)]), args.at(1)])
          when :&, :|, :^
            literal(SQL::Function.new(BITWISE_METHOD_MAP[op], *args))
          when :<<
            "(#{literal(args[0])} * POWER(2, #{literal(args[1])}))"
          when :>>
            "(#{literal(args[0])} / POWER(2, #{literal(args[1])}))"
          else
            super(op, args)
          end
        end
        
        # H2 requires SQL standard datetimes
        def requires_sql_standard_datetimes?
          true
        end

        # H2 doesn't support IS TRUE
        def supports_is_true?
          false
        end
        
        # H2 doesn't support JOIN USING
        def supports_join_using?
          false
        end
        
        # H2 doesn't support multiple columns in IN/NOT IN
        def supports_multiple_column_in?
          false
        end 

        private
      
        # H2 expects hexadecimal strings for blob values
        def literal_blob(v)
          literal_string v.unpack("H*").first
        end
        
        def convert_type(v)
          case v
          when Java::OrgH2Jdbc::JdbcClob
            convert_type(v.getSubString(1, v.length))
          else
            super(v)
          end
        end
        
        def select_clause_methods
          SELECT_CLAUSE_METHODS
        end
      end
    end
  end
end
