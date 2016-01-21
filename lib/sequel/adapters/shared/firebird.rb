# frozen-string-literal: true

module Sequel
  module Firebird
    module DatabaseMethods
      extend Sequel::Database::ResetIdentifierMangling

      AUTO_INCREMENT = ''.freeze
      TEMPORARY = 'GLOBAL TEMPORARY '.freeze

      def clear_primary_key(*tables)
        tables.each{|t| @primary_keys.delete(dataset.send(:input_identifier, t))}
      end

      def create_trigger(*args)
        self << create_trigger_sql(*args)
      end

      def database_type
        :firebird
      end

      def drop_sequence(name)
        self << drop_sequence_sql(name)
      end

      # Return primary key for the given table.
      def primary_key(table)
        t = dataset.send(:input_identifier, table)
        @primary_keys.fetch(t) do
          pk = fetch("SELECT RDB$FIELD_NAME FROM RDB$INDEX_SEGMENTS NATURAL JOIN RDB$RELATION_CONSTRAINTS WHERE RDB$CONSTRAINT_TYPE = 'PRIMARY KEY' AND RDB$RELATION_NAME = ?", t).single_value
          @primary_keys[t] = dataset.send(:output_identifier, pk.rstrip) if pk
        end
      end

      def restart_sequence(*args)
        self << restart_sequence_sql(*args)
      end

      def sequences(opts=OPTS)
        ds = self[:"rdb$generators"].server(opts[:server]).filter(:"rdb$system_flag" => 0).select(:"rdb$generator_name")
        block_given? ? yield(ds) : ds.map{|r| ds.send(:output_identifier, r[:"rdb$generator_name"])}
      end

      def tables(opts=OPTS)
        tables_or_views(0, opts)
      end

      def views(opts=OPTS)
        tables_or_views(1, opts)
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
      
      def create_sequence_sql(name, opts=OPTS)
        "CREATE SEQUENCE #{quote_identifier(name)}"
      end

      # Firebird gets an override because of the mess of creating a
      # sequence and trigger for auto-incrementing primary keys.
      def create_table_from_generator(name, generator, options)
        drop_statement, create_statements = create_table_sql_list(name, generator, options)
        (execute_ddl(drop_statement) rescue nil) if drop_statement
        create_statements.each{|sql| execute_ddl(sql)}
      end

      def create_table_sql_list(name, generator, options=OPTS)
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

      def create_trigger_sql(table, name, definition, opts=OPTS)
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
      
      def drop_sequence_sql(name)
        "DROP SEQUENCE #{quote_identifier(name)}"
      end

      def remove_cached_schema(table)
        clear_primary_key(table)
        super
      end

      def restart_sequence_sql(name, opts=OPTS)
        seq_name = quote_identifier(name)
        "ALTER SEQUENCE #{seq_name} RESTART WITH #{opts[:restart_position]}"
      end

      def tables_or_views(type, opts)
        ds = self[:"rdb$relations"].server(opts[:server]).filter(:"rdb$relation_type" => type, Sequel::SQL::Function.new(:COALESCE, :"rdb$system_flag", 0) => 0).select(:"rdb$relation_name")
        ds.map{|r| ds.send(:output_identifier, r[:"rdb$relation_name"].rstrip)}
      end

      def type_literal_generic_string(column)
        column[:text] ? :"BLOB SUB_TYPE TEXT" : super
      end

      # Firebird supports views with check option, but not local.
      def view_with_check_option_support
        true
      end
    end

    module DatasetMethods
      BOOL_TRUE = '1'.freeze
      BOOL_FALSE = '0'.freeze
      NULL = LiteralString.new('NULL').freeze
      FIRST = " FIRST ".freeze
      SKIP = " SKIP ".freeze
      DEFAULT_FROM = " FROM RDB$DATABASE"
      
      Dataset.def_sql_method(self, :select, %w'with select distinct limit columns from join where group having compounds order')
      Dataset.def_sql_method(self, :insert, %w'insert into columns values returning')

      # Insert given values into the database.
      def insert(*values)
        if @opts[:sql] || @opts[:returning]
          super
        else
          returning(insert_pk).insert(*values){|r| return r.values.first}
        end
      end

      # Insert a record returning the record inserted
      def insert_select(*values)
        with_sql_first(insert_select_sql(*values))
      end

      # The SQL to use for an insert_select, adds a RETURNING clause to the insert
      # unless the RETURNING clause is already present.
      def insert_select_sql(*values)
        ds = opts[:returning] ? self : returning
        ds.insert_sql(*values)
      end

      def requires_sql_standard_datetimes?
        true
      end

      def supports_cte?(type=:select)
        type == :select
      end

      def supports_insert_select?
        true
      end

      # Firebird does not support INTERSECT or EXCEPT
      def supports_intersect_except?
        false
      end

      def supports_returning?(type)
        type == :insert
      end

      private

      def empty_from_sql
        DEFAULT_FROM
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

      # Firebird can insert multiple rows using a UNION
      def multi_insert_sql_strategy
        :union
      end

      def select_limit_sql(sql)
        if l = @opts[:limit]
          sql << FIRST
          literal_append(sql, l)
        end
        if o = @opts[:offset]
          sql << SKIP
          literal_append(sql, o)
        end
      end
    end
  end
end
