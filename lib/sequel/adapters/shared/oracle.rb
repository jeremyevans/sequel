Sequel.require 'adapters/utils/emulate_offset_with_row_number'

module Sequel
  module Oracle
    module DatabaseMethods
      TEMPORARY = 'GLOBAL TEMPORARY '.freeze
      AUTOINCREMENT = ''.freeze

      attr_accessor :autosequence

      def create_sequence(name, opts={})
        self << create_sequence_sql(name, opts)
      end

      def create_trigger(*args)
        self << create_trigger_sql(*args)
      end

      def current_user
        @current_user ||= get{sys_context('USERENV', 'CURRENT_USER')}
      end

      def drop_sequence(name)
        self << drop_sequence_sql(name)
      end

      # Oracle uses the :oracle database type
      def database_type
        :oracle
      end

      def tables(opts={})
        ds = from(:tab).server(opts[:server]).select(:tname).filter(:tabtype => 'TABLE')
        ds.map{|r| ds.send(:output_identifier, r[:tname])}
      end

      def views(opts={}) 
        ds = from(:tab).server(opts[:server]).select(:tname).filter(:tabtype => 'VIEW') 
        ds.map{|r| ds.send(:output_identifier, r[:tname])} 
      end 
 
      def view_exists?(name) 
        from(:tab).filter(:tname =>dataset.send(:input_identifier, name), :tabtype => 'VIEW').count > 0 
      end 

      private

      # Handle Oracle specific ALTER TABLE SQL
      def alter_table_sql(table, op)
        case op[:op]
        when :add_column
          if op[:primary_key]
            sqls = []
            sqls << alter_table_sql(table, op.merge(:primary_key=>nil))
            if op[:auto_increment]
              seq_name = default_sequence_name(table, op[:name])
              sqls << drop_sequence_sql(seq_name)
              sqls << create_sequence_sql(seq_name, op)
              sqls << "UPDATE #{quote_schema_table(table)} SET #{quote_identifier(op[:name])} = #{seq_name}.nextval"
            end
            sqls << "ALTER TABLE #{quote_schema_table(table)} ADD PRIMARY KEY (#{quote_identifier(op[:name])})"
            sqls
          else
             "ALTER TABLE #{quote_schema_table(table)} ADD #{column_definition_sql(op)}"
          end
        when :set_column_null
          "ALTER TABLE #{quote_schema_table(table)} MODIFY #{quote_identifier(op[:name])} #{op[:null] ? 'NULL' : 'NOT NULL'}"
        when :set_column_type
          "ALTER TABLE #{quote_schema_table(table)} MODIFY #{quote_identifier(op[:name])} #{type_literal(op)}"
        when :set_column_default
          "ALTER TABLE #{quote_schema_table(table)} MODIFY #{quote_identifier(op[:name])} DEFAULT #{literal(op[:default])}"
        else
          super(table, op)
        end
      end

      def auto_increment_sql
        AUTOINCREMENT
      end

      def create_sequence_sql(name, opts={})
        "CREATE SEQUENCE #{quote_identifier(name)} start with #{opts [:start_with]||1} increment by #{opts[:increment_by]||1} nomaxvalue"
      end

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
            c[:sequence_name] ||= default_sequence_name(name, c[:name])
            unless c[:create_sequence] == false
              drop_seq_statement = drop_sequence_sql(c[:sequence_name])
              statements << create_sequence_sql(c[:sequence_name], c)
            end
            unless c[:create_trigger] == false
              c[:trigger_name] ||= "BI_#{name}_#{c[:name]}"
              trigger_definition = <<-end_sql
              BEGIN
                IF :NEW.#{quote_identifier(c[:name])} IS NULL THEN
                  SELECT #{c[:sequence_name]}.nextval INTO :NEW.#{quote_identifier(c[:name])} FROM dual;
                END IF;
              END;
              end_sql
              statements << create_trigger_sql(name, c[:trigger_name], trigger_definition, {:events => [:insert]})
            end
          end
        end
        [drop_seq_statement, statements]
      end

      def create_trigger_sql(table, name, definition, opts={})
        events = opts[:events] ? Array(opts[:events]) : [:insert, :update, :delete]
        sql = <<-end_sql
          CREATE#{' OR REPLACE' if opts[:replace]} TRIGGER #{quote_identifier(name)}
          #{opts[:after] ? 'AFTER' : 'BEFORE'} #{events.map{|e| e.to_s.upcase}.join(' OR ')} ON #{quote_schema_table(table)}
          REFERENCING NEW AS NEW FOR EACH ROW
          #{definition}
        end_sql
        sql
      end

      def default_sequence_name(table, column)
        "seq_#{table}_#{column}"
      end

      def drop_sequence_sql(name)
        "DROP SEQUENCE #{quote_identifier(name)}"
      end

      def remove_cached_schema(table)
        @primary_key_sequences.delete(table)
        super
      end
      
      def sequence_for_table(table)
        return nil unless autosequence
        @primary_key_sequences.fetch(table) do |key|
          pk = schema(table).select{|k, v| v[:primary_key]}
          @primary_key_sequences[table] = if pk.length == 1
            seq = "seq_#{table}_#{pk.first.first}"
            seq.to_sym unless from(:user_sequences).filter(:sequence_name=>input_identifier_meth.call(seq)).empty?
          end
        end
      end

      # Oracle's integer/:number type handles larger values than
      # most other databases's bigint types, so it should be
      # safe to use for Bignum.
      def type_literal_generic_bignum(column)
        :integer
      end

      # Oracle doesn't have a time type, so use timestamp for all
      # time columns.
      def type_literal_generic_time(column)
        :timestamp
      end

      # Oracle doesn't have a boolean type or even a reasonable
      # facsimile.  Using a char(1) seems to be the recommended way.
      def type_literal_generic_trueclass(column)
        :'char(1)'
      end

      # SQL fragment for showing a table is temporary
      def temporary_table_sql
        TEMPORARY
      end
    end

    module DatasetMethods
      include EmulateOffsetWithRowNumber

      SELECT_CLAUSE_METHODS = Dataset.clause_methods(:select, %w'with distinct columns from join where group having compounds order lock')
      ROW_NUMBER_EXPRESSION = 'ROWNUM'.lit.freeze

      # Oracle needs to emulate bitwise operators and ILIKE/NOT ILIKE operators.
      def complex_expression_sql(op, args)
        case op
        when :&
          complex_expression_arg_pairs(args){|a, b| "CAST(BITAND(#{literal(a)}, #{literal(b)}) AS INTEGER)"}
        when :|
          complex_expression_arg_pairs(args){|a, b| "(#{literal(a)} - #{complex_expression_sql(:&, [a, b])} + #{literal(b)})"}
        when :^
          complex_expression_arg_pairs(args){|*x| "(#{complex_expression_sql(:|, x)} - #{complex_expression_sql(:&, x)})"}
        when :'B~'
          "((0 - #{literal(args.at(0))}) - 1)"
        when :<<
          complex_expression_arg_pairs(args){|a, b| "(#{literal(a)} * power(2, #{literal b}))"}
        when :>>
          complex_expression_arg_pairs(args){|a, b| "(#{literal(a)} / power(2, #{literal b}))"}
        when :ILIKE, :'NOT ILIKE'
          a, b = args
          "(UPPER(#{literal(a)}) #{op == :ILIKE ? :LIKE : :'NOT LIKE'} UPPER(#{literal(b)}))"
        else
          super
        end
      end

      # Oracle doesn't support CURRENT_TIME, as it doesn't have
      # a type for storing just time values without a date, so
      # use CURRENT_TIMESTAMP in its place.
      def constant_sql(c)
        if c == :CURRENT_TIME
          super(:CURRENT_TIMESTAMP)
        else
          super
        end
      end

      # Oracle uses MINUS instead of EXCEPT, and doesn't support EXCEPT ALL
      def except(dataset, opts={})
        opts = {:all=>opts} unless opts.is_a?(Hash)
        raise(Sequel::Error, "EXCEPT ALL not supported") if opts[:all]
        compound_clone(:minus, dataset, opts)
      end

      def empty?
        db[:dual].where(unordered.exists).get(1) == nil
      end

      # Oracle requires SQL standard datetimes
      def requires_sql_standard_datetimes?
        true
      end

      # Create a copy of this dataset associated to the given sequence name,
      # which will be used when calling insert to find the most recently
      # inserted value for the sequence.
      def sequence(s)
        clone(:sequence=>s)
      end

      # Handle LIMIT by using a unlimited subselect filtered with ROWNUM.
      def select_sql
        if (limit = @opts[:limit]) && !@opts[:sql]
          ds = clone(:limit=>nil)
          # Lock doesn't work in subselects, so don't use a subselect when locking.
          # Don't use a subselect if custom SQL is used, as it breaks somethings.
          ds = ds.from_self unless @opts[:lock]
          subselect_sql(ds.where(SQL::ComplexExpression.new(:<=, ROW_NUMBER_EXPRESSION, limit)))
        else
          super
        end
      end

      # Oracle requires recursive CTEs to have column aliases.
      def recursive_cte_requires_column_aliases?
        true
      end

      # Oracle does not support INTERSECT ALL or EXCEPT ALL
      def supports_intersect_except_all?
        false
      end

      # Oracle does not support IS TRUE.
      def supports_is_true?
        false
      end
      
      # Oracle does not support SELECT *, column
      def supports_select_all_and_column?
        false
      end
      
      # Oracle supports timezones in literal timestamps.
      def supports_timestamp_timezones?
        true
      end
      
      # Oracle does not support WHERE 'Y' for WHERE TRUE.
      def supports_where_true?
        false
      end

      # Oracle supports window functions
      def supports_window_functions?
        true
      end

      private

      # Oracle doesn't support the use of AS when aliasing a dataset.  It doesn't require
      # the use of AS anywhere, so this disables it in all cases.
      def as_sql(expression, aliaz)
        "#{expression} #{quote_identifier(aliaz)}"
      end

      # The strftime format to use when literalizing the time.
      def default_timestamp_format
        "TIMESTAMP '%Y-%m-%d %H:%M:%S%N %z'".freeze
      end

      # If this dataset is associated with a sequence, return the most recently
      # inserted sequence value.
      def execute_insert(sql, opts={})
        f = @opts[:from]
        super(sql, {:table=>(f.first if f), :sequence=>@opts[:sequence]}.merge(opts))
      end

      # Use a colon for the timestamp offset, since Oracle appears to require it.
      def format_timestamp_offset(hour, minute)
        sprintf("%+03i:%02i", hour, minute)
      end

      # Oracle doesn't support empty values when inserting.
      def insert_supports_empty_values?
        false
      end

      # Use string in hex format for blob data.
      def literal_blob(v)
        blob = "'"
        v.each_byte{|x| blob << sprintf('%02x', x)}
        blob << "'"
        blob
      end

      # Oracle uses 'N' for false values.
      def literal_false
        "'N'"
      end

      # Oracle uses the SQL standard of only doubling ' inside strings.
      def literal_string(v)
        "'#{v.gsub("'", "''")}'"
      end

      # Oracle uses 'Y' for true values.
      def literal_true
        "'Y'"
      end

      # Use the Oracle-specific SQL clauses (no limit, since it is emulated).
      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end

      # Modify the SQL to add the list of tables to select FROM
      # Oracle doesn't support select without FROM clause
      # so add the dummy DUAL table if the dataset doesn't select
      # from a table.
      def select_from_sql(sql)
        sql << " FROM #{source_list(@opts[:from] || ['DUAL'])}"
      end
    end
  end
end
