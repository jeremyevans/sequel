module Sequel
  module ODBC
    # Database and Dataset instance methods for DB2 specific
    # support via ODBC.
    module DB2
      module DatabaseMethods
        def dataset(opts=nil)
          Sequel::ODBC::DB2::Dataset.new(self, opts)
        end
      end
      
      class Dataset < ODBC::Dataset
        def select_limit_sql(sql)
          if l = @opts[:limit]
            sql << " FETCH FIRST #{l == 1 ? 'ROW' : "#{literal(l)} ROWS"} ONLY"
          end
        end
      end
    end
  end
end
