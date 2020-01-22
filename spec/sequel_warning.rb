if ENV['WARNING']
  require 'warning'
  Warning.ignore([:missing_ivar, :method_redefined, :not_reached], File.dirname(File.dirname(__FILE__)))
  Warning.dedup if Warning.respond_to?(:dedup)
end
