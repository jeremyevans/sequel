module Sequel
  MAJOR = 3
  MINOR = 0
  TINY  = 0
  
  VERSION = [MAJOR, MINOR, TINY].join('.')
  
  # The version of Sequel you are using, as a string (e.g. "2.11.0")
  def self.version
    VERSION
  end
end
