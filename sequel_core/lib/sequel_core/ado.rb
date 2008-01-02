warn "Requiring 'sequel/ado' is deprecated. Please modify your code to only require 'sequel' instead."

if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../sequel')
end
require File.join(File.dirname(__FILE__), 'adapters/ado')
