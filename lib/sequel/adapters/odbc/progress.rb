Sequel.require 'adapters/shared/progress'

Sequel.synchronize do
  Sequel::ODBC::DATABASE_SETUP[:progress] = proc do |db|
    db.extend Sequel::Progress::DatabaseMethods
    db.extend_datasets(Sequel::Progress::DatasetMethods)
  end
end
