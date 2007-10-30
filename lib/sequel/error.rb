# Represents an error raised in Sequel code.
class SequelError < StandardError
end

# SequelRollbackError is a special error used to rollback a transactions.
# A transaction block will catch this error and wont pass further up the stack.
class SequelRollbackError < StandardError
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
    raise SequelRollbackError
  end
end