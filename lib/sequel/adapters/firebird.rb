require 'fb'

module Sequel
  # The Sequel Firebird adapter requires the ruby fb driver located at
  # http://github.com/wishdev/fb.
  module Firebird
    class Database < Sequel::Database
      set_adapter_scheme :firebird

      AUTO_INCREMENT = ''.freeze
      TEMPORARY = 'GLOBAL TEMPORARY '.freeze

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
        conn.downcase_names = true
        conn
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
            r = log_yield(sql){conn.execute(sql)}
            yield(r) if block_given?
            r
          end
        rescue => e
          raise_error(e, :classes=>[Fb::Error])
        end
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

      def restart_sequence(*args)
        self << restart_sequence_sql(*args)
      end

      def sequences(opts={})
        ds = self[:"rdb$generators"].server(opts[:server]).filter(:"rdb$system_flag" => 0).select(:"rdb$generator_name")
        block_given? ? yield(ds) : ds.map{|r| ds.send(:output_identifier, r[:"rdb$generator_name"])}
      end

      def tables(opts={})
        ds = self[:"rdb$relations"].server(opts[:server]).filter(:"rdb$view_blr" => nil, Sequel::SQL::Function.new(:COALESCE, :"rdb$system_flag", 0) => 0).select(:"rdb$relation_name")
        block_given? ? yield(ds) : ds.map{|r| ds.send(:output_identifier, r[:"rdb$relation_name"])}
      end

      private

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
      
      def begin_transaction(conn)
        log_yield(TRANSACTION_BEGIN){conn.transaction}
        conn
      end

      def commit_transaction(conn)
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
        position = opts[:position] ? opts[:position] : 0
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
      
      def rollback_transaction(conn)
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

      # Yield all rows returned by executing the given SQL and converting
      # the types.
      def fetch_rows(sql, &block)
        execute(sql) do |s|
          begin
            @columns = s.fields.map{|c| output_identifier(c.name)}
            s.fetchall(:symbols_hash).each do |r|
              h = {}
              r.each{|k,v| h[output_identifier(k)] = v}
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
        if !@opts[:sql]
          clone(default_server_opts(:sql=>insert_returning_pk_sql(*values))).single_value
        else
          execute_insert(insert_sql(*values), :table=>opts[:from].first,
            :values=>values.size == 1 ? values.first : values)
        end
      end

      # Use the RETURNING clause to return the primary key of the inserted record, if it exists
      def insert_returning_pk_sql(*values)
        pk = db.primary_key(opts[:from].first)
        insert_returning_sql(pk ? Sequel::SQL::Identifier.new(pk) : NULL, *values)
      end

      # Use the RETURNING clause to return the columns listed in returning.
      def insert_returning_sql(returning, *values)
        "#{insert_sql(*values)} RETURNING #{column_list(Array(returning))}"
      end

      # Insert a record returning the record inserted
      def insert_select(*values)
        naked.clone(default_server_opts(:sql=>insert_returning_sql(nil, *values))).single_record
      end
      
      def requires_sql_standard_datetimes?
        true
      end

      # The order of clauses in the SELECT SQL statement
      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end
      
      def select_limit_sql(sql)
        sql << " FIRST #{@opts[:limit]}" if @opts[:limit]
        sql << " SKIP #{@opts[:offset]}" if @opts[:offset]
      end

      # Firebird does not support INTERSECT or EXCEPT
      def supports_intersect_except?
        false
      end

      private

      def hash_row(stmt, row)
        @columns.inject({}) do |m, c|
          m[c] = row.shift
          m
        end
      end

      def literal_false
        BOOL_FALSE
      end

      def literal_true
        BOOL_TRUE
      end
    end
  end
end
