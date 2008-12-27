require 'fb'

module Sequel
  module Firebird
    CONVERTED_EXCEPTIONS = [Fb::Error]

    class Database < Sequel::Database

      set_adapter_scheme :firebird

      # Add the primary_keys and primary_key_sequences instance variables,
      # so we can get the correct return values for inserted rows.
      def initialize(*args)
        super
        @primary_keys = {}
        @primary_key_sequences = {}
      end

      
      def connect(server)
        opts = server_opts(server)

        db = Fb::Database.new(
          :database => "#{opts[:host]}:#{opts[:database]}",
          :username => opts[:user],
          :password => opts[:password])
        conn = db.connect
        conn
      end

      # Return primary key for the given table.
      def primary_key(table, server=nil)
        synchronize(server){|conn| primary_key_for_table(conn, table)}
      end

      # Returns primary key for the given table.  This information is
      # cached, and if the primary key for a table is changed, the
      # @primary_keys instance variable should be reset manually.
      def primary_key_for_table(conn, table)
        @primary_keys[quote_identifier(table)] ||= conn.table_primary_key(quote_identifier(table))
      end

      def dataset(opts = nil)
        Firebird::Dataset.new(self, opts)
      end

      def execute(sql, opts={})
        log_info(sql)
        begin
          synchronize(opts[:server]) do |conn|
            r = conn.execute(sql)
            yield(r) if block_given?
            r
          end
        rescue => e
          log_info(e.message)
          raise_error(e, :classes=>CONVERTED_EXCEPTIONS)
        end
      end

      def tables(opts={})
        ds = self["RDB$RELATIONS".intern]
        ds = ds.filter({"RDB$VIEW_BLR".intern => nil} & {:COALESCE["RDB$SYSTEM_FLAG".intern, 0] => 0})
        ds = ds.select("RDB$RELATION_NAME".intern)

        block_given? ? yield(ds) : ds.map{|r| r["RDB$RELATION_NAME".intern].intern}
      end

      def transaction(server=nil)
        synchronize(server) do |conn|
          return yield(conn) if @transactions.include?(Thread.current)
          log_info("Begin transaction")
          conn.transaction
          begin
            @transactions << Thread.current
            yield(conn)
          rescue ::Exception => e
            log_info("Rolling back")
            conn.rollback
            transaction_error(e, Fb::Error)
          ensure
            unless e
              log_info("Commiting")
              conn.commit
            end
            @transactions.delete(Thread.current)
          end
        end
      end

      AUTO_INCREMENT = ''.freeze

      def auto_increment_sql()
        AUTO_INCREMENT
      end

      # Use Firebird specific syntax for add column
      def alter_table_sql(table, op)
        case op[:op]
        when :add_column
          "ALTER TABLE #{quote_schema_table(table)} ADD #{column_definition_sql(op)}"
        when :drop_column
          "ALTER TABLE #{quote_schema_table(table)} DROP #{column_definition_sql(op)}"
        when :rename_column
          "ALTER TABLE #{quote_schema_table(table)} ALTER #{quote_identifier(op[:name])} TO #{quote_identifier(op[:new_name])}"
        when :set_column_type
          "ALTER TABLE #{quote_schema_table(table)} ALTER #{quote_identifier(op[:name])} TYPE #{type_literal(op)}"
        else
          super(table, op)
        end
      end

      def create_sequence_sql(name)
        "CREATE SEQUENCE #{quote_identifier(name)}"
      end

      def create_sequence_trigger_sql(table, col, seq)
        table = quote_identifier(table)
        col = quote_identifier(col)
        seq = quote_identifier(seq)
        sql = <<-end_sql
          CREATE TRIGGER BI_#{table}_#{col} for #{table}
          active before insert position 0
          as
          begin
            if ((new.#{col} is null) or (new.#{col} = 0)) then
            begin
              new.#{col} = next value for #{seq};
            end
          end
        end_sql
        sql
      end

      def drop_sequence_sql(name)
        "DROP SEQUENCE #{quote_identifier(name)}"
      end

      def create_table_sql_list(name, columns, indexes = nil)
        pre_statements = []
        statements = super
        columns.each do |c|
          if c[:auto_increment]
            pre_statements << drop_sequence_sql("seq_#{name}_#{c[:name]}")
            statements << create_sequence_sql("seq_#{name}_#{c[:name]}")
            statements << create_sequence_trigger_sql(name, c[:name], "seq_#{name}_#{c[:name]}")
            break
          end
        end
        pre_statements.concat(statements)
      end

      private

      def disconnect_connection(c)
        c.close
      end
    end

    # Dataset class for Firebird datasets
    class Dataset < Sequel::Dataset
      include UnsupportedIntersectExcept

      BOOL_TRUE = '1'.freeze
      BOOL_FALSE = '0'.freeze
      COMMA_SEPARATOR = ', '.freeze
      SELECT_CLAUSE_ORDER = %w'distinct limit columns from join where group having union order'.freeze

      FIREBIRD_TIMESTAMP_FORMAT = "TIMESTAMP '%Y-%m-%d %H:%M:%S".freeze

      def literal(v)
        case v
        when Time, DateTime
          "#{v.strftime(FIREBIRD_TIMESTAMP_FORMAT)}.#{sprintf("%04d",v.usec / 100)}'"
        when TrueClass
          BOOL_TRUE
        when FalseClass
          BOOL_FALSE
        else
          super
        end
      end


      # Yield all rows returned by executing the given SQL and converting
      # the types.
      def fetch_rows(sql, &block)
        execute(sql) do |s|
          begin
            @columns = s.fields.map do |c|
              c.name.intern
            end
            s.fetchall(:symbols_hash).each{ |r| yield r}
          ensure
            s.close
          end
        end
        self
      end

      # Insert given values into the database.
      def insert(*values)
        if !@opts[:sql]
          single_value(:sql=>insert_returning_pk_sql(*values))
        else
          execute_insert(insert_sql(*values), :table=>opts[:from].first,
            :values=>values.size == 1 ? values.first : values)
        end
      end

      # Use the RETURNING clause to return the columns listed in returning.
      def insert_returning_sql(returning, *values)
        "#{insert_sql(*values)} RETURNING #{column_list(Array(returning))}"
      end


      # Insert a record returning the record inserted
      def insert_select(*values)
        single_record(:naked=>true, :sql=>insert_returning_sql(nil, *values))
      end

      # Use the RETURNING clause to return the primary key of the inserted record, if it exists
      def insert_returning_pk_sql(*values)
        pk = db.primary_key(opts[:from].first)
        insert_returning_sql(pk ? Sequel::SQL::Identifier.new(pk) : 'NULL'.lit, *values)
      end

      # The order of clauses in the SELECT SQL statement
      def select_clause_order
        SELECT_CLAUSE_ORDER
      end
      
      def select_limit_sql(sql, opts)
        sql << " FIRST #{opts[:limit]}" if opts[:limit]
        sql << " SKIP #{opts[:offset]}" if opts[:offset]
      end

      private

      def hash_row(stmt, row)
        @columns.inject({}) do |m, c|
          m[c] = row.shift
          m
        end
      end
    end
  end
end