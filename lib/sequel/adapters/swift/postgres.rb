Sequel.require 'adapters/shared/postgres'

module Sequel
  
  module Swift
    Postgres::CONVERTED_EXCEPTIONS << ::Swift::Error
    # Adapter, Database, and Dataset support for accessing a PostgreSQL
    # database via Swift.
    module Postgres
      # Methods to add to Database instances that access PostgreSQL via Swift.
      module DatabaseMethods
        include Sequel::Postgres::DatabaseMethods
        
        # Add the primary_keys and primary_key_sequences instance variables,
        # so we can get the correct return values for inserted rows.
        def self.extended(db)
          db.send(:initialize_postgres_adapter)
        end
        
        private
        
        # Remove all other options except for ones specifically handled, as
        # otherwise swift passes them to dbic++ which passes them to PostgreSQL
        # which can raise an error.
        def server_opts(o)
          o = super
          so = {}
          [:db, :user, :password, :host, :port].each{|s| so[s] = o[s] if o.has_key?(s)}
          so
        end
      
        # Extend the adapter with the Swift PostgreSQL AdapterMethods.
        def setup_connection(conn)
          conn = super(conn)
          connection_configuration_sqls.each{|sql| log_yield(sql){conn.execute(sql)}}
          conn
        end
      end
    end
  end
end
