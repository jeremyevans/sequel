warn "Requiring 'sequel/sqlite' is deprecated. Please modify your code to require 'sequel' instead."
require File.join(File.dirname(__FILE__), 'adapters/sqlite')
