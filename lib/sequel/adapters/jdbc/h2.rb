Sequel::JDBC.load_driver('org.h2.Driver', :H2)

module Sequel
  module JDBC
    Sequel.synchronize do
      DATABASE_SETUP[:h2] = proc do |db|
        db.extend(Sequel::JDBC::H2::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::H2::Dataset
        org.h2.Driver
      end
    end

    # Database and Dataset support for H2 databases accessed via JDBC.
    module H2
      # Instance methods for H2 Database objects accessed via JDBC.
      module DatabaseMethods
        extend Sequel::Database::ResetIdentifierMangling
        PRIMARY_KEY_INDEX_RE = /\Aprimary_key/i.freeze
      
        # Commit an existing prepared transaction with the given transaction
        # identifier string.
        def commit_prepared_transaction(transaction_id, opts=OPTS)
          run("COMMIT TRANSACTION #{transaction_id}", opts)
        end

        # H2 uses the :h2 database type.
        def database_type
          :h2
        end

        # Rollback an existing prepared transaction with the given transaction
        # identifier string.
        def rollback_prepared_transaction(transaction_id, opts=OPTS)
          run("ROLLBACK TRANSACTION #{transaction_id}", opts)
        end

        # H2 uses an IDENTITY type
        def serial_primary_key_options
          {:primary_key => true, :type => :identity, :identity=>true}
        end

        # H2 supports CREATE TABLE IF NOT EXISTS syntax.
        def supports_create_table_if_not_exists?
          true
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
        def commit_transaction(conn, opts=OPTS)
          if (s = opts[:prepare]) && savepoint_level(conn) <= 1
            log_connection_execute(conn, "PREPARE COMMIT #{s}")
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
              sqls << "ALTER TABLE #{quote_schema_table(table)} ADD PRIMARY KEY (#{quote_identifier(op[:name])})" if pk && op[:type] != :identity
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
            if sch = schema(table)
              if cs = sch.each{|k, v| break v if k == op[:name]; nil}
                cs = cs.dup
                cs[:default] = cs[:ruby_default]
                op = cs.merge!(op)
              end
            end
            sql = "ALTER TABLE #{quote_schema_table(table)} ALTER COLUMN #{quote_identifier(op[:name])} #{type_literal(op)}"
            column_definition_order.each{|m| send(:"column_definition_#{m}_sql", sql, op)}
            sql
          when :drop_constraint
            if op[:type] == :primary_key
              "ALTER TABLE #{quote_schema_table(table)} DROP PRIMARY KEY"
            else
              super(table, op)
            end
          else
            super(table, op)
          end
        end
        
        # Default to a single connection for a memory database.
        def connection_pool_default_options
          o = super
          uri == 'jdbc:h2:mem:' ? o.merge(:max_connections=>1) : o
        end
      
        DATABASE_ERROR_REGEXPS = {
          /Unique index or primary key violation/ => UniqueConstraintViolation,
          /Referential integrity constraint violation/ => ForeignKeyConstraintViolation,
          /Check constraint violation/ => CheckConstraintViolation,
          /NULL not allowed for column/ => NotNullConstraintViolation,
          /Deadlock detected\. The current transaction was rolled back\./ => SerializationFailure,
        }.freeze
        def database_error_regexps
          DATABASE_ERROR_REGEXPS
        end

        # Use IDENTITY() to get the last inserted id.
        def last_insert_id(conn, opts=OPTS)
          statement(conn) do |stmt|
            sql = 'SELECT IDENTITY();'
            rs = log_yield(sql){stmt.executeQuery(sql)}
            rs.next
            rs.getLong(1)
          end
        end
        
        def primary_key_index_re
          PRIMARY_KEY_INDEX_RE
        end

        # H2 does not support named column constraints.
        def supports_named_column_constraints?
          false
        end

        # Use BIGINT IDENTITY for identity columns that use bigint, fixes
        # the case where primary_key :column, :type=>Bignum is used.
        def type_literal_generic_bignum(column)
          column[:identity] ? 'BIGINT IDENTITY' : super
        end
      end
      
      # Dataset class for H2 datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        APOS = Dataset::APOS
        HSTAR = "H*".freeze
        ILIKE_PLACEHOLDER = ["CAST(".freeze, " AS VARCHAR_IGNORECASE)".freeze].freeze
        TIME_FORMAT = "'%H:%M:%S'".freeze
        ONLY_OFFSET = " LIMIT -1 OFFSET ".freeze

        # Emulate the case insensitive LIKE operator and the bitwise operators.
        def complex_expression_sql_append(sql, op, args)
          case op
          when :ILIKE, :"NOT ILIKE"
            super(sql, (op == :ILIKE ? :LIKE : :"NOT LIKE"), [SQL::PlaceholderLiteralString.new(ILIKE_PLACEHOLDER, [args.at(0)]), args.at(1)])
          when :&, :|, :^, :<<, :>>, :'B~'
            complex_expression_emulate_append(sql, op, args)
          else
            super
          end
        end
        
        # H2 does not support derived column lists
        def supports_derived_column_lists?
          false
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
        def literal_blob_append(sql, v)
          sql << APOS << v.unpack(HSTAR).first << APOS
        end
        
        # H2 handles fractional seconds in timestamps, but not in times
        def literal_sqltime(v)
          v.strftime(TIME_FORMAT)
        end

        # H2 supports multiple rows in INSERT.
        def multi_insert_sql_strategy
          :values
        end

        def select_only_offset_sql(sql)
          sql << ONLY_OFFSET
          literal_append(sql, @opts[:offset])
        end

        # H2 supports quoted function names.
        def supports_quoted_function_names?
          true
        end
      end
    end
  end
end
