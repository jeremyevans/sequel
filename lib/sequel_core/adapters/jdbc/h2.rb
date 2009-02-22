require 'sequel_core/adapters/utils/date_format'

module Sequel
  module JDBC
    # Database and Dataset support for H2 databases accessed via JDBC.
    module H2
      # Instance methods for H2 Database objects accessed via JDBC.
      module DatabaseMethods
        # H2 needs to add a primary key column as a constraint
        def alter_table_sql(table, op)
          case op[:op]
          when :add_column
            if op.delete(:primary_key)
              sql = super(table, op)
              [sql, "ALTER TABLE #{quote_schema_table(table)} ADD PRIMARY KEY (#{quote_identifier(op[:name])})"]
            else
              super(table, op)
            end
          else
            super(table, op)
          end
        end
      
        # Return Sequel::JDBC::H2::Dataset object with the given opts.
        def dataset(opts=nil)
          Sequel::JDBC::H2::Dataset.new(self, opts)
        end
        
        # H2 uses an IDENTITY type
        def serial_primary_key_options
          {:primary_key => true, :type => :identity}
        end
        
        private
        
        # Use IDENTITY() to get the last inserted id.
        def last_insert_id(conn, opts={})
          stmt = conn.createStatement
          begin
            rs = stmt.executeQuery('SELECT IDENTITY();')
            rs.next
            rs.getInt(1)
          ensure
            stmt.close
          end 
        end
        
        # Default to a single connection for a memory database.
        def connection_pool_default_options
          o = super
          uri == 'jdbc:h2:mem:' ? o.merge(:max_connections=>1) : o
        end
      end
      
      # Dataset class for H2 datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Dataset::SQLStandardDateFormat
      end
    end
  end
end
