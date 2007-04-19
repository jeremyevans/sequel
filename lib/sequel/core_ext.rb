# Enumerable extensions.
module Enumerable
  def send_each(sym, *args)
    each {|i| i.send(sym, *args)}
  end
end