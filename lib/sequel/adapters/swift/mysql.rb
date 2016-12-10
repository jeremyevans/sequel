# frozen-string-literal: true

require 'swift/db/mysql'
Sequel.require 'adapters/shared/mysql'

module Sequel
  module Swift
    # Database and Dataset instance methods for MySQL specific
    # support via Swift.
    module MySQL
      # Database instance methods for MySQL databases accessed via Swift.
      module DatabaseMethods
        include Sequel::MySQL::DatabaseMethods
        
        private
        
        # The database name for the given database.
        def database_name
          opts[:database]
        end

        # Consider tinyint(1) columns as boolean.
        def schema_column_type(db_type)
          db_type =~ /\Atinyint\(1\)/ ? :boolean : super
        end
      
        # Apply the connectiong setting SQLs for every new connection.
        def setup_connection(conn)
          mysql_connection_setting_sqls.each{|sql| log_connection_yield(sql, conn){conn.execute(sql)}}
          super
        end
      end
      
      # Dataset class for MySQL datasets accessed via Swift.
      class Dataset < Swift::Dataset
        include Sequel::MySQL::DatasetMethods
        APOS = Dataset::APOS
        
        private
        
        # Use Swift's escape method for quoting.
        def literal_string_append(sql, s)
          sql << APOS << db.synchronize(@opts[:server]){|c| c.escape(s)} << APOS
        end
      end
    end
  end
end
