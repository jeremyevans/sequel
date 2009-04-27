Sequel.require %w'date_format unsupported', 'adapters/utils'

module Sequel
  module Oracle
    module DatabaseMethods
      TEMPORARY = 'GLOBAL TEMPORARY '.freeze

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

      # SQL fragment for showing a table is temporary
      def temporary_table_sql
        TEMPORARY
      end
    end
    
    module DatasetMethods
      include Dataset::UnsupportedIntersectExceptAll
      include Dataset::SQLStandardDateFormat

      SELECT_CLAUSE_ORDER = %w'distinct columns from join where group having compounds order limit'.freeze

      # Oracle doesn't support DISTINCT ON
      def distinct(*columns)
        raise(Error, "DISTINCT ON not supported by Oracle") unless columns.empty?
        super
      end

      # Oracle uses MINUS instead of EXCEPT, and doesn't support EXCEPT ALL
      def except(dataset, all = false)
        raise(Sequel::Error, "EXCEPT ALL not supported") if all
        compound_clone(:minus, dataset, all)
      end

      def empty?
        db[:dual].where(exists).get(1) == nil
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
