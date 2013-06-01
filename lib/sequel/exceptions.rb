module Sequel
  # The default exception class for exceptions raised by Sequel.
  # All exception classes defined by Sequel are descendants of this class.
  class Error < ::StandardError
    # If this exception wraps an underlying exception, the underlying
    # exception is held here.
    attr_accessor :wrapped_exception
  end  
    
  # Error raised when the adapter requested doesn't exist or can't be loaded.
  class AdapterNotFound < Error; end

  # Generic error raised by the database adapters, indicating a
  # problem originating from the database server.  Usually raised
  # because incorrect SQL syntax is used.
  class DatabaseError < Error; end
  
  # Error raised when the Sequel is unable to connect to the database with the
  # connection parameters it was given.
  class DatabaseConnectionError < DatabaseError; end

  # Error raised by adapters when they determine that the connection
  # to the database has been lost.  Instructs the connection pool code to 
  # remove that connection from the pool so that other connections can be acquired
  # automatically.
  class DatabaseDisconnectError < DatabaseError; end

  # Generic error raised when Sequel determines a database constraint has been violated.
  class ConstraintViolation < DatabaseError; end

  # Error raised when Sequel determines a database check constraint has been violated.
  class CheckConstraintViolation < ConstraintViolation; end

  # Error raised when Sequel determines a database foreign key constraint has been violated.
  class ForeignKeyConstraintViolation < ConstraintViolation; end

  # Error raised when Sequel determines a database NOT NULL constraint has been violated.
  class NotNullConstraintViolation < ConstraintViolation; end

  # Error raised when Sequel determines a database unique constraint has been violated.
  class UniqueConstraintViolation < ConstraintViolation; end

  # Error raised when Sequel determines a serialization failure/deadlock in the database.
  class SerializationFailure < DatabaseError; end

  # Error raised on an invalid operation, such as trying to update or delete
  # a joined or grouped dataset.
  class InvalidOperation < Error; end

  # Error raised when attempting an invalid type conversion.
  class InvalidValue < Error; end

  # Error raised when the user requests a record via the first! or similar
  # method, and the dataset does not yield any rows.
  class NoMatchingRow < Error; end

  # Error raised when the connection pool cannot acquire a database connection
  # before the timeout.
  class PoolTimeout < Error; end

  # Error that you should raise to signal a rollback of the current transaction.
  # The transaction block will catch this exception, rollback the current transaction,
  # and won't reraise it (unless a reraise is requested).
  class Rollback < Error; end

  # Error raised when unbinding a dataset that has multiple different values
  # for a given variable.
  class UnbindDuplicate < Error; end

  class Error
    AdapterNotFound = Sequel::AdapterNotFound
    InvalidOperation = Sequel::InvalidOperation
    InvalidValue = Sequel::InvalidValue
    PoolTimeoutError = Sequel::PoolTimeout
    Rollback = Sequel::Rollback
  end  
end
