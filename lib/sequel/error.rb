class SequelError < StandardError
end

# This error class is used to wrap exceptions occuring inside calls to
# ConnectionPool#hold. Sequel wraps any exception raised by the database
# connection and provides it as a SequelConnectionError. The original
# exception is provided through SequelConnectionError#original_exception.
class SequelConnectionError < SequelError
  attr_reader :original_error

  def initialize(original_error)
    @original_error = original_error
  end
  
  def message
    "#{@original_error.class}: #{@original_error.message}"
  end
  
  alias_method :to_s, :message
end
