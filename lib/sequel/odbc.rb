warn "Requiring 'sequel/odbc' is deprecated. Please modify your code to require 'sequel' instead."

if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../sequel')
end
require File.join(File.dirname(__FILE__), 'adapters/odbc')
