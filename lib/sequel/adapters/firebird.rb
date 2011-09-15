require 'fb'

module Sequel
  # The Sequel Firebird adapter requires the ruby fb driver located at
  # http://github.com/wishdev/fb.
  module Firebird
    class Database < Sequel::Database
      set_adapter_scheme :firebird

      AUTO_INCREMENT = ''.freeze
      TEMPORARY = 'GLOBAL TEMPORARY '.freeze
      DISCONNECT_ERRORS = /Unsuccessful execution caused by a system error that precludes successful execution of subsequent statements/

      # Add the primary_keys instance variables.
      # so we can get the correct return values for inserted rows.
      def initialize(*args)
        super
        @primary_keys = {}
      end

      def connect(server)
        opts = server_opts(server)

        Fb::Database.new(
          :database => "#{opts[:host]}:#{opts[:database]}",
          :username => opts[:user],
          :password => opts[:password]).connect
      end

      def create_trigger(*args)
        self << create_trigger_sql(*args)
      end

      def dataset(opts = nil)
        Firebird::Dataset.new(self, opts)
      end

      def drop_sequence(name)
        self << drop_sequence_sql(name)
      end

      def execute(sql, opts={})
        begin
          synchronize(opts[:server]) do |conn|
            if conn.transaction_started && !@transactions.include?(Thread.current)
              conn.rollback
              raise DatabaseDisconnectError, "transaction accidently left open, rolling back and disconnecting"
            end
            r = log_yield(sql){conn.execute(sql)}
            yield(r) if block_given?
            r
          end
        rescue Fb::Error => e
          raise_error(e, :disconnect=>DISCONNECT_ERRORS.match(e.message))
        end
      end

      # Return primary key for the given table.
      def primary_key(table)
        t = dataset.send(:input_identifier, table)
        @primary_keys.fetch(t) do
          pk = fetch("SELECT RDB$FIELD_NAME FROM RDB$INDEX_SEGMENTS NATURAL JOIN RDB$RELATION_CONSTRAINTS WHERE RDB$CONSTRAINT_TYPE = 'PRIMARY KEY' AND RDB$RELATION_NAME = ?", t).single_value
          @primary_keys[t] = dataset.send(:output_identifier, pk.rstrip) if pk
        end
      end

      def drop_table(*names)
        clear_primary_key(*names)
        super
      end

      def clear_primary_key(*tables)
        tables.each{|t| @primary_keys.delete(dataset.send(:input_identifier, t))}
      end

      def restart_sequence(*args)
        self << restart_sequence_sql(*args)
      end

      def sequences(opts={})
        ds = self[:"rdb$generators"].server(opts[:server]).filter(:"rdb$system_flag" => 0).select(:"rdb$generator_name")
        block_given? ? yield(ds) : ds.map{|r| ds.send(:output_identifier, r[:"rdb$generator_name"])}
      end

      def tables(opts={})
        tables_or_views(0, opts)
      end

      def views(opts={})
        tables_or_views(1, opts)
      end

      private

      def tables_or_views(type, opts)
        ds = self[:"rdb$relations"].server(opts[:server]).filter(:"rdb$relation_type" => type, Sequel::SQL::Function.new(:COALESCE, :"rdb$system_flag", 0) => 0).select(:"rdb$relation_name")
        ds.map{|r| ds.send(:output_identifier, r[:"rdb$relation_name"].rstrip)}
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

      def auto_increment_sql()
        AUTO_INCREMENT
      end
      
      def begin_transaction(conn, opts={})
        log_yield(TRANSACTION_BEGIN) do
          begin
            conn.transaction
          rescue Fb::Error => e
            conn.rollback
            raise_error(e, :disconnect=>true) 
          end
        end
        conn
      end

      def commit_transaction(conn, opts={})
        log_yield(TRANSACTION_COMMIT){conn.commit}
      end
      
      def create_sequence_sql(name, opts={})
        "CREATE SEQUENCE #{quote_identifier(name)}"
      end

      # Firebird gets an override because of the mess of creating a
      # sequence and trigger for auto-incrementing primary keys.
      def create_table_from_generator(name, generator, options)
        drop_statement, create_statements = create_table_sql_list(name, generator, options)
        (execute_ddl(drop_statement) rescue nil) if drop_statement
        create_statements.each{|sql| execute_ddl(sql)}
      end

      def create_table_sql_list(name, generator, options={})
        statements = [create_table_sql(name, generator, options)]
        drop_seq_statement = nil
        generator.columns.each do |c|
          if c[:auto_increment]
            c[:sequence_name] ||= "seq_#{name}_#{c[:name]}"
            unless c[:create_sequence] == false
              drop_seq_statement = drop_sequence_sql(c[:sequence_name])
              statements << create_sequence_sql(c[:sequence_name])
              statements << restart_sequence_sql(c[:sequence_name], {:restart_position => c[:sequence_start_position]}) if c[:sequence_start_position]
            end
            unless c[:create_trigger] == false
              c[:trigger_name] ||= "BI_#{name}_#{c[:name]}"
              c[:quoted_name] = quote_identifier(c[:name])
              trigger_definition = <<-END
              begin
                if ((new.#{c[:quoted_name]} is null) or (new.#{c[:quoted_name]} = 0)) then
                begin
                  new.#{c[:quoted_name]} = next value for #{c[:sequence_name]};
                end
              end
              END
              statements << create_trigger_sql(name, c[:trigger_name], trigger_definition, {:events => [:insert]})
            end
          end
        end
        [drop_seq_statement, statements]
      end

      def create_trigger_sql(table, name, definition, opts={})
        events = opts[:events] ? Array(opts[:events]) : [:insert, :update, :delete]
        whence = opts[:after] ? 'AFTER' : 'BEFORE'
        inactive = opts[:inactive] ? 'INACTIVE' : 'ACTIVE'
        position = opts.fetch(:position, 0)
        sql = <<-end_sql
          CREATE TRIGGER #{quote_identifier(name)} for #{quote_identifier(table)}
          #{inactive} #{whence} #{events.map{|e| e.to_s.upcase}.join(' OR ')} position #{position}
          as #{definition}
        end_sql
        sql
      end
      
      def database_error_classes
        [Fb::Error]
      end

      def disconnect_connection(c)
        c.close
      end

      def drop_sequence_sql(name)
        "DROP SEQUENCE #{quote_identifier(name)}"
      end

      def restart_sequence_sql(name, opts={})
        seq_name = quote_identifier(name)
        "ALTER SEQUENCE #{seq_name} RESTART WITH #{opts[:restart_position]}"
      end
      
      def rollback_transaction(conn, opts={})
        log_yield(TRANSACTION_ROLLBACK){conn.rollback}
      end

      def type_literal_generic_string(column)
        column[:text] ? :"BLOB SUB_TYPE TEXT" : super
      end
    end

    # Dataset class for Firebird datasets
    class Dataset < Sequel::Dataset
      BOOL_TRUE = '1'.freeze
      BOOL_FALSE = '0'.freeze
      NULL = LiteralString.new('NULL').freeze
      COMMA_SEPARATOR = ', '.freeze
      SELECT_CLAUSE_METHODS = clause_methods(:select, %w'with distinct limit columns from join where group having compounds order')
      INSERT_CLAUSE_METHODS = clause_methods(:insert, %w'into columns values returning')

      # Yield all rows returned by executing the given SQL and converting
      # the types.
      def fetch_rows(sql)
        execute(sql) do |s|
          begin
            @columns = columns = s.fields.map{|c| output_identifier(c.name)}
            s.fetchall.each do |r|
              h = {}
              r.zip(columns).each{|v, c| h[c] = v}
              yield h
            end
          ensure
            s.close
          end
        end
        self
      end

      # Insert given values into the database.
      def insert(*values)
        if @opts[:sql] || @opts[:returning]
          super
        elsif supports_insert_select?
          returning(insert_pk).insert(*values){|r| return r.values.first}
        end
      end

      # Insert a record returning the record inserted
      def insert_select(*values)
        returning.insert(*values){|r| return r}
      end

      def requires_sql_standard_datetimes?
        true
      end

      def supports_insert_select?
        true
      end

      # Firebird does not support INTERSECT or EXCEPT
      def supports_intersect_except?
        false
      end

      private

      def insert_clause_methods
        INSERT_CLAUSE_METHODS
      end

      def insert_pk(*values)
        pk = db.primary_key(opts[:from].first)
        pk ? Sequel::SQL::Identifier.new(pk) : NULL
      end

      def literal_false
        BOOL_FALSE
      end

      def literal_true
        BOOL_TRUE
      end

      # The order of clauses in the SELECT SQL statement
      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end
      
      def select_limit_sql(sql)
        sql << " FIRST #{@opts[:limit]}" if @opts[:limit]
        sql << " SKIP #{@opts[:offset]}" if @opts[:offset]
      end
    end
  end
end
