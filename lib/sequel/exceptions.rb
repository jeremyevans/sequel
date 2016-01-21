# frozen-string-literal: true

module Sequel
  # The default exception class for exceptions raised by Sequel.
  # All exception classes defined by Sequel are descendants of this class.
  class Error < ::StandardError
    # If this exception wraps an underlying exception, the underlying
    # exception is held here.
    attr_accessor :wrapped_exception
  end  
    
  # Error raised when the adapter requested doesn't exist or can't be loaded.
  AdapterNotFound = Class.new(Error)

  # Generic error raised by the database adapters, indicating a
  # problem originating from the database server.  Usually raised
  # because incorrect SQL syntax is used.
  DatabaseError = Class.new(Error)
  
  # Error raised when the Sequel is unable to connect to the database with the
  # connection parameters it was given.
  DatabaseConnectionError = Class.new(DatabaseError)

  # Error raised by adapters when they determine that the connection
  # to the database has been lost.  Instructs the connection pool code to 
  # remove that connection from the pool so that other connections can be acquired
  # automatically.
  DatabaseDisconnectError = Class.new(DatabaseError)

  # Generic error raised when Sequel determines a database constraint has been violated.
  ConstraintViolation = Class.new(DatabaseError)

  # Error raised when Sequel determines a database check constraint has been violated.
  CheckConstraintViolation = Class.new(ConstraintViolation)

  # Error raised when Sequel determines a database foreign key constraint has been violated.
  ForeignKeyConstraintViolation = Class.new(ConstraintViolation)

  # Error raised when Sequel determines a database NOT NULL constraint has been violated.
  NotNullConstraintViolation = Class.new(ConstraintViolation)

  # Error raised when Sequel determines a database unique constraint has been violated.
  UniqueConstraintViolation = Class.new(ConstraintViolation)

  # Error raised when Sequel determines a serialization failure/deadlock in the database.
  SerializationFailure = Class.new(DatabaseError)

  # Error raised on an invalid operation, such as trying to update or delete
  # a joined or grouped dataset.
  InvalidOperation = Class.new(Error)

  # Error raised when attempting an invalid type conversion.
  InvalidValue = Class.new(Error)

  # Error raised when the user requests a record via the first! or similar
  # method, and the dataset does not yield any rows.
  class NoMatchingRow < Error
    # The dataset that raised this NoMatchingRow exception.
    attr_accessor :dataset

    # If the first argument is a Sequel::Dataset, set the dataset related to
    # the exception to that argument, instead of assuming it is the exception message.
    def initialize(msg=nil)
      if msg.is_a?(Sequel::Dataset)
        @dataset = msg
        msg = nil
      end
      super
    end
  end

  # Error raised when the connection pool cannot acquire a database connection
  # before the timeout.
  PoolTimeout = Class.new(Error)

  # Error that you should raise to signal a rollback of the current transaction.
  # The transaction block will catch this exception, rollback the current transaction,
  # and won't reraise it (unless a reraise is requested).
  Rollback = Class.new(Error)

  # Error raised when unbinding a dataset that has multiple different values
  # for a given variable.
  UnbindDuplicate = Class.new(Error)

  # Call name on each class to set the name for the class, so it gets cached.
  constants.map{|c| const_get(c)}.each do |c|
    Class === c && c.name
  end

  Error::AdapterNotFound = AdapterNotFound
  Error::InvalidOperation = InvalidOperation
  Error::InvalidValue = InvalidValue
  Error::PoolTimeoutError = PoolTimeout
  Error::Rollback = Rollback
end
