require File.join(File.dirname(__FILE__), '../lib/sequel')

class SchemaDummyDatabase < Sequel::Database
  attr_reader :sqls
  
  # def execute(sql)
  #   @sqls ||= []
  #   @sqls << sql
  # end
end