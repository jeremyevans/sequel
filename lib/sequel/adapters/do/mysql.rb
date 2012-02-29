Sequel.require 'adapters/shared/mysql'

module Sequel
  module DataObjects
    # Database and Dataset instance methods for MySQL specific
    # support via DataObjects.
    module MySQL
      # Database instance methods for MySQL databases accessed via DataObjects.
      module DatabaseMethods
        include Sequel::MySQL::DatabaseMethods
        
        private
        
        # The database name for the given database.  Need to parse it out
        # of the connection string, since the DataObjects does no parsing on the
        # given connection string by default.
        def database_name
          (m = /\/(.*)/.match(URI.parse(uri).path)) && m[1]
        end

        def schema_column_type(db_type)
          db_type == 'tinyint(1)' ? :boolean : super
        end
      end
      
      # Dataset class for MySQL datasets accessed via DataObjects.
      class Dataset < DataObjects::Dataset
        include Sequel::MySQL::DatasetMethods
        APOS = Dataset::APOS
        APOS_RE = Dataset::APOS_RE
        DOUBLE_APOS = Dataset::DOUBLE_APOS
        
        # The DataObjects MySQL driver uses the number of rows actually modified in the update,
        # instead of the number of matched by the filter.
        def provides_accurate_rows_matched?
          false
        end
      
        # Use execute_insert to execute the replace_sql.
        def replace(*args)
          execute_insert(replace_sql(*args))
        end
        
        private
        
        # do_mysql sets NO_BACKSLASH_ESCAPES, so use standard SQL string escaping
        def literal_string_append(sql, s)
          sql << APOS << s.gsub(APOS_RE, DOUBLE_APOS) << APOS
        end
      end
    end
  end
end
