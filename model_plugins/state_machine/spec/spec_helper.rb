require "sequel"
require File.join(File.dirname(__FILE__), "../lib/sequel_state_machine")

class Symbol
  def to_proc() lambda{ |object, *args| object.send(self, *args) } end
end
