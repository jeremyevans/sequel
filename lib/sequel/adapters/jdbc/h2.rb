Sequel.require %w'date_format unsupported', 'adapters/utils'

module Sequel
  module JDBC
    # Database and Dataset support for H2 databases accessed via JDBC.
    module H2
      # Instance methods for H2 Database objects accessed via JDBC.
      module DatabaseMethods
        PRIMARY_KEY_INDEX_RE = /\Aprimary_key/i.freeze
      
        # Return Sequel::JDBC::H2::Dataset object with the given opts.
        def dataset(opts=nil)
          Sequel::JDBC::H2::Dataset.new(self, opts)
        end
        
        # H2 uses an IDENTITY type
        def serial_primary_key_options
          {:primary_key => true, :type => :identity}
        end
        
        private
        
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
        
        # Default to a single connection for a memory database.
        def connection_pool_default_options
          o = super
          uri == 'jdbc:h2:mem:' ? o.merge(:max_connections=>1) : o
        end
      
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
        
        def primary_key_index_re
          PRIMARY_KEY_INDEX_RE
        end
      end
      
      # Dataset class for H2 datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Dataset::SQLStandardDateFormat
        include Dataset::UnsupportedIsTrue
      end
    end
  end
end
