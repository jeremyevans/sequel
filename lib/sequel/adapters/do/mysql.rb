Sequel::DataObjects.load_driver 'do_mysql'
Sequel.require 'adapters/shared/mysql'

module Sequel
  module DataObjects
    Sequel.synchronize do
      DATABASE_SETUP[:mysql] = proc do |db|
        db.extend(Sequel::DataObjects::MySQL::DatabaseMethods)
        db.dataset_class = Sequel::DataObjects::MySQL::Dataset
      end
    end

    # Database and Dataset instance methods for MySQL specific
    # support via DataObjects.
    module MySQL
      # Database instance methods for MySQL databases accessed via DataObjects.
      module DatabaseMethods
        extend Sequel::Database::ResetIdentifierMangling
        include Sequel::MySQL::DatabaseMethods
        
        private
        
        # The database name for the given database.  Need to parse it out
        # of the connection string, since the DataObjects does no parsing on the
        # given connection string by default.
        def database_name
          (m = /\/(.*)/.match(URI.parse(uri).path)) && m[1]
        end

        # Recognize the tinyint(1) column as boolean.
        def schema_column_type(db_type)
          db_type =~ /\Atinyint\(1\)/ ? :boolean : super
        end

        # Apply the connectiong setting SQLs for every new connection.
        def setup_connection(conn)
          mysql_connection_setting_sqls.each{|sql| log_yield(sql){conn.create_command(sql).execute_non_query}}
          super
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
      
        private
        
        # do_mysql sets NO_BACKSLASH_ESCAPES, so use standard SQL string escaping
        def literal_string_append(sql, s)
          sql << APOS << s.gsub(APOS_RE, DOUBLE_APOS) << APOS
        end
      end
    end
  end
end
