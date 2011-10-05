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

      def auto_increment_sql
        AUTOINCREMENT
      end

      def column_definition_order
        super + [:check]
      end

      def column_definition_sql(column)
        if (column[:type] == FalseClass || column[:type] == TrueClass)
          super({:check=>{column[:name]=>%w[Y N]}}.merge(column))
        else
          super
        end
      end

      def column_definition_check_sql(sql, column)
        sql << " CHECK #{literal(column[:check])}" if column[:check]
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
            c[:sequence_name] ||= "seq_#{name}_#{c[:name]}"
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

      def drop_sequence_sql(name)
        "DROP SEQUENCE #{quote_identifier(name)}"
      end

      def type_literal_generic_trueclass(column)
        :'char(1)'
      end

      # SQL fragment for showing a table is temporary
      def temporary_table_sql
        TEMPORARY
      end
    end

    module DatasetMethods
      SELECT_CLAUSE_METHODS = Dataset.clause_methods(:select, %w'with distinct columns from join where group having compounds order limit lock')

      def complex_expression_sql(op, args)
        case op
        when :&
          "CAST(BITAND#{literal(args)} AS INTEGER)"
        when :|
          a, b = args
          "(#{literal(a)} - #{complex_expression_sql(:&, args)} + #{literal(b)})"
        when :^
          "(#{complex_expression_sql(:|, args)} - #{complex_expression_sql(:&, args)})"
        when :'B~'
          "((0 - #{literal(args.at(0))}) - 1)"
        when :<<
          a, b = args
          "(#{literal(a)} * power(2, #{literal b}))"
        when :>>
          a, b = args
          "(#{literal(a)} / power(2, #{literal b}))"
        when :ILIKE, :'NOT ILIKE'
          a, b = args
          "(UPPER(#{literal(a)}) #{op == :ILIKE ? :LIKE : :'NOT LIKE'} UPPER(#{literal(b)}))"
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
        db[:dual].where(exists).get(1) == nil
      end

      # If this dataset is associated with a sequence, return the most recently
      # inserted sequence value.
      def insert(*values)
        if opts[:sql]
          super
        else
          execute_insert(insert_sql(*values), :table=>opts[:from].first, :sequence=>opts[:sequence])
        end
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

      # Oracle does not support INTERSECT ALL or EXCEPT ALL
      def supports_intersect_except_all?
        false
      end

      def supports_is_true?
        false
      end
      
      # Oracle supports timezones in literal timestamps.
      def supports_timestamp_timezones?
        true
      end
      
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

      # Use a colon for the timestamp offset, since Oracle appears to require it.
      def format_timestamp_offset(hour, minute)
        sprintf("%+03i:%02i", hour, minute)
      end

      def literal_false
        "'N'"
      end

      # Oracle uses the SQL standard of only doubling ' inside strings.
      def literal_string(v)
        "'#{v.gsub("'", "''")}'"
      end

      def literal_true
        "'Y'"
      end

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

      # Oracle requires a subselect to do limit and offset
      def select_limit_sql(sql)
        if limit = @opts[:limit]
          if (offset = @opts[:offset]) && (offset > 0)
            sql.replace("SELECT * FROM (SELECT raw_sql_.*, ROWNUM raw_rnum_ FROM(#{sql}) raw_sql_ WHERE ROWNUM <= #{limit + offset}) WHERE raw_rnum_ > #{offset}")
          else
            sql.replace("SELECT * FROM (#{sql}) WHERE ROWNUM <= #{limit}")
          end
        end
      end
    end
  end
end
