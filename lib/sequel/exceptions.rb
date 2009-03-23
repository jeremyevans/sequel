module Sequel
  # Represents an error raised in Sequel code.
  class Error < ::StandardError
    
    # Raised when Sequel is unable to load a specified adapter.
    class AdapterNotFound < Error ; end

    # Raised on an invalid operation.
    class InvalidOperation < Error; end
                                       
    # Represents an Invalid transform.
    class InvalidTransform < Error ; end
    
    # Represents an invalid value stored in the database.
    class InvalidValue < Error ; end
                                       
    # Represents an attempt to performing filter operations when no filter has been specified yet.
    class NoExistingFilter < Error ; end
                                       
    # There was an error while waiting on a connection from the connection pool
    class PoolTimeoutError < Error ; end
                                       
    # Rollback is a special error used to rollback a transactions.
    # A transaction block will catch this error and won't pass further up the stack.
    class Rollback < Error ; end
  end  

  # Generic error raised by the database adapters, indicating a
  # problem originating from the database server.
  class DatabaseError < Error; end
  
  # Error raised when the Sequel is unable to connect to the database with the
  # connection parameters it was given.
  class DatabaseConnectionError < DatabaseError; end

  # Error that should be raised by adapters when they determine that the connection
  # to the database has been lost.  Instructs the connection pool code to 
  # remove that connection from the pool so that other connections can be acquired
  # automatically.
  class DatabaseDisconnectError < DatabaseError; end
end
