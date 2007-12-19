module Sequel
  # Represents an error raised in Sequel code.
  class Error < StandardError

    # Rollback is a special error used to rollback a transactions.
    # A transaction block will catch this error and wont pass further up the stack.
    class Rollback            < Error ; end  

    # Represents an invalid value stored in the database.
    class InvalidValue                 < Error ; end
                                       
    # Represents invalid arguments p   assed to sequel methods.
    class Argument                     < Error ; end
                                       
    # Represents Invalid parameters    passed to a sequel method.
    class InvalidParameters            < Error ; end
                                       
    # Represents an Invalid transfor   m.
    class InvalidTransform             < Error ; end
                                       
    # Represents an Invalid filter.    
    class InvalidFilter                < Error ; end
                                       
    # Represents a failure to provid   e a connection proc for the connection pool.
    class NoConnectionProc             < Error ; end
                                       
    # Represents missing a required    connection string.
    class NoConnectionString           < Error ; end
                                       
    # Represents an attempt to perfo   rming filter operations when no filter has been specified yet.
    class NoExistingFilter             < Error ; end
                                       
    # Represents an invalid join typ   e.
    class InvalidJoinType              < Error ; end
                                       
    # Represents an attempt to perfo   rm an update on a grouped dataset.
    class UpdateGroupedDataset         < Error ; end
                                       
    # Represents an attempt to perfo   rm an update on a joined dataset.
    class UpdateJoinedDataset          < Error ; end
                                       
    # Represents an attempt to perfo   rm an delete from a grouped dataset.
    class DeleteGroupedDataset         < Error ; end
                                       
    # Represents an attempt to perfo   rm an delete from a joined dataset.
    class DeleteJoinedDataset          < Error ; end
                                       
    class InvalidMigrationDirection    < Error ; end
                                       
    class NoCurrentVersionAvailable    < Error ; end
                                       
    class NoTargetVersionAvailable     < Error ; end
                                       
    class OffsetNotSupported           < Error ; end
    
    # Represents a model that has no associated dataset.
    class NoDatasetAssociatedWithModel < Error ; end
    
    # Represents a model with no primary key specified.
    class NoPrimaryKeyForModel         < Error ; end
    
    class UnsupportedMatchPatternClass < Error ; end
    
    class Index                        < Error ; end

    class Name                         < Error ; end

    class InvalidExpression            < Error ; end

    class InvalidExpressionTree        < Error ; end

    class ChainBroken < RuntimeError ; end

    class WorkerStopError < RuntimeError ; end

  end  
end

# Object extensions
class Object
  # Cancels the current transaction without an error:
  #
  #   DB.tranaction do
  #     ...
  #     rollback! if failed_to_contact_client
  #     ...
  #   end
  def rollback!
    raise Sequel::RollbackError
  end
end
