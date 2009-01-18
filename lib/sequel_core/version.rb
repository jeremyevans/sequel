module Sequel
  MAJOR = 2
  MINOR = 9
  TINY  = 0
  
  VERSION = [MAJOR, MINOR, TINY].join('.')
  
  def self.version
    VERSION
  end
end
