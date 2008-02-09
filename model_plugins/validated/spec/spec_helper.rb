require "#{ File.dirname(__FILE__) }/../lib/validated.rb"

def subject() ::Validated end
def h(something)
  puts '<pre>%s</pre>' %
  something.inspect.gsub(/[<>]/) {|m| (m == '<')? '&lt;': '&gt;'}
end
