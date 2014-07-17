Sequel.require 'adapters/shared/db2'

Sequel.synchronize do
  Sequel::ODBC::DATABASE_SETUP[:db2] = proc do |db|
    db.extend ::Sequel::DB2::DatabaseMethods
    db.extend_datasets ::Sequel::DB2::DatasetMethods
  end
end

