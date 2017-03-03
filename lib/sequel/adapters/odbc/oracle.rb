# frozen-string-literal: true

Sequel.require 'adapters/shared/oracle'

Sequel.synchronize do
  Sequel::ODBC::DATABASE_SETUP[:oracle] = proc do |db|
    db.extend ::Sequel::Oracle::DatabaseMethods
    db.extend_datasets ::Sequel::Oracle::DatasetMethods
  end
end

