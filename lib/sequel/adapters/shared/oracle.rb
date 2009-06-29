module Sequel
  module Oracle
    module DatabaseMethods
      TEMPORARY = 'GLOBAL TEMPORARY '.freeze
      AUTOINCREMENT = ''.freeze

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

      def table_exists?(name)
        from(:tab).filter(:tname =>dataset.send(:input_identifier, name), :tabtype => 'TABLE').count > 0
      end

      private

      def auto_increment_sql
        AUTOINCREMENT
      end

      # SQL fragment for showing a table is temporary
      def temporary_table_sql
        TEMPORARY
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
    end
    
    module DatasetMethods
      SELECT_CLAUSE_ORDER = %w'with distinct columns from join where group having compounds order limit'.freeze

      # Oracle uses MINUS instead of EXCEPT, and doesn't support EXCEPT ALL
      def except(dataset, all = false)
        raise(Sequel::Error, "EXCEPT ALL not supported") if all
        compound_clone(:minus, dataset, all)
      end

      def empty?
        db[:dual].where(exists).get(1) == nil
      end

      # Oracle requires SQL standard datetimes
      def requires_sql_standard_datetimes?
        true
      end

      # Oracle does not support DISTINCT ON
      def supports_distinct_on?
        false
      end

      # Oracle does not support INTERSECT ALL or EXCEPT ALL
      def supports_intersect_except_all?
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

      # Oracle uses the SQL standard of only doubling ' inside strings.
      def literal_string(v)
        "'#{v.gsub("'", "''")}'"
      end

      def select_clause_order
        SELECT_CLAUSE_ORDER
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
