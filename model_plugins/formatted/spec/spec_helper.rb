require "sequel"
require File.join(File.dirname(__FILE__), "../lib/sequel_formatted")

class Symbol
  def to_proc() lambda{ |object, *args| object.send(self, *args) } end
end
