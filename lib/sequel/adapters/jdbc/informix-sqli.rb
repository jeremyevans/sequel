# frozen-string-literal: true

Sequel::JDBC.load_driver('com.informix.jdbc.IfxDriver')
Sequel.require 'adapters/shared/informix'

module Sequel
  module JDBC
    Sequel.synchronize do
      DATABASE_SETUP[:"informix-sqli"] = proc do |db|
        db.extend(Sequel::JDBC::Informix::DatabaseMethods)
        db.extend_datasets Sequel::Informix::DatasetMethods
        com.informix.jdbc.IfxDriver
      end
    end

    # Database and Dataset instance methods for Informix specific
    # support via JDBC.
    module Informix
      # Database instance methods for Informix databases accessed via JDBC.
      module DatabaseMethods
        include Sequel::Informix::DatabaseMethods
        
        private
        
        # TODO: implement
        def last_insert_id(conn, opts=OPTS)
          nil
        end
      end
    end
  end
end
