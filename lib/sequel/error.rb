class SequelError < StandardError
end

class SequelRollbackError < StandardError
end

class Object
  def rollback
    raise SequelRollbackError
  end
end