if ENV['WARNING']
  require 'warning'
  Warning.ignore([:missing_ivar, :method_redefined], File.dirname(File.dirname(__FILE__)))
  Warning.dedup if Warning.respond_to?(:dedup)
end
