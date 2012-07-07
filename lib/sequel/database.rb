module Sequel
  # Hash of adapters that have been used. The key is the adapter scheme
  # symbol, and the value is the Database subclass.
  ADAPTER_MAP = {}

  # Array of all databases to which Sequel has connected.  If you are
  # developing an application that can connect to an arbitrary number of
  # databases, delete the database objects from this or they will not get
  # garbage collected.
  DATABASES = []

  # A Database object represents a virtual connection to a database.
  # The Database class is meant to be subclassed by database adapters in order
  # to provide the functionality needed for executing queries.
  class Database
    extend Metaprogramming
    include Metaprogramming
  end

  require(%w"connecting dataset dataset_defaults logging misc query schema_generator schema_methods", 'database')
end
