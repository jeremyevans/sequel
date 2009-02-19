require 'fb'

module Sequel
  # The Sequel Firebird adapter requires the ruby fb driver located at
  # http://github.com/wishdev/fb.
  module Firebird
    class Database < Sequel::Database
      set_adapter_scheme :firebird

      AUTO_INCREMENT = ''.freeze

      # Add the primary_keys and primary_key_sequences instance variables,
      # so we can get the correct return values for inserted rows.
      def initialize(*args)
        super
        @primary_keys = {}
        @primary_key_sequences = {}
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

      def create_sequence_sql(name, opts={})
        "CREATE SEQUENCE #{quote_identifier(name)}"
      end

      # Creates a table with the columns given in the provided block:
      #
      #   DB.create_table :posts do
      #     primary_key :id, :serial
      #     column :title, :text
      #     column :content, :text
      #     index :title
      #   end
      #
      # See Schema::Generator.
      # Firebird gets an override because of the mess of creating a
      # generator for auto-incrementing primary keys.
      def create_table(name, options={}, &block)
        options = {:generator=>options} if options.is_a?(Schema::Generator)
        statements = create_table_sql_list(name, *((options[:generator] ||= Schema::Generator.new(self, &block)).create_info << options))
        begin
          execute_ddl(statements[1])
        rescue
          nil
        end if statements[1]
        statements[0].flatten.each {|sql| execute_ddl(sql)}
      end

      def create_table_sql_list(name, columns, indexes = nil, options={})
        statements = super
        drop_seq_statement = nil
        columns.each do |c|
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
        [statements, drop_seq_statement]
      end

      def create_trigger(*args)
        self << create_trigger_sql(*args)
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

      def dataset(opts = nil)
        Firebird::Dataset.new(self, opts)
      end

      def drop_sequence(name)
        self << drop_sequence_sql(name)
      end

      def drop_sequence_sql(name)
        "DROP SEQUENCE #{quote_identifier(name)}"
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

      def restart_sequence_sql(name, opts={})
        seq_name = quote_identifier(name)
        "ALTER SEQUENCE #{seq_name} RESTART WITH #{opts[:restart_position]}"
      end

      def sequences(opts={})
        ds = self[:"rdb$generators"].server(opts[:server]).filter(:"rdb$system_flag" => 0).select(:"rdb$generator_name")
        block_given? ? yield(ds) : ds.map{|r| ds.send(:output_identifier, r[:"rdb$generator_name"])}
      end

      def tables(opts={})
        ds = self[:"rdb$relations"].server(opts[:server]).filter(:"rdb$view_blr" => nil, Sequel::SQL::Function.new(:COALESCE, :"rdb$system_flag", 0) => 0).select(:"rdb$relation_name")
        block_given? ? yield(ds) : ds.map{|r| ds.send(:output_identifier, r[:"rdb$relation_name"])}
      end

      def transaction(server=nil)
        synchronize(server) do |conn|
          return yield(conn) if @transactions.include?(Thread.current)
          log_info("Transaction.begin")
          conn.transaction
          begin
            @transactions << Thread.current
            yield(conn)
          rescue ::Exception => e
            log_info("Transaction.rollback")
            conn.rollback
            transaction_error(e, Fb::Error)
          ensure
            unless e
              log_info("Transaction.commit")
              conn.commit
            end
            @transactions.delete(Thread.current)
          end
        end
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
      FIREBIRD_TIMESTAMP_FORMAT = "TIMESTAMP '%Y-%m-%d %H:%M:%S".freeze
      SELECT_CLAUSE_ORDER = %w'distinct limit columns from join where group having compounds order'.freeze

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
          single_value(default_server_opts(:sql=>insert_returning_pk_sql(*values)))
        else
          execute_insert(insert_sql(*values), :table=>opts[:from].first,
            :values=>values.size == 1 ? values.first : values)
        end
      end

      # Use the RETURNING clause to return the primary key of the inserted record, if it exists
      def insert_returning_pk_sql(*values)
        pk = db.primary_key(opts[:from].first)
        insert_returning_sql(pk ? Sequel::SQL::Identifier.new(pk) : 'NULL'.lit, *values)
      end

      # Use the RETURNING clause to return the columns listed in returning.
      def insert_returning_sql(returning, *values)
        "#{insert_sql(*values)} RETURNING #{column_list(Array(returning))}"
      end

      # Insert a record returning the record inserted
      def insert_select(*values)
        single_record(default_server_opts(:naked=>true, :sql=>insert_returning_sql(nil, *values)))
      end

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
