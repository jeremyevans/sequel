if ENV['WARNING']
  require 'warning'
  Warning.ignore(:missing_ivar, File.dirname(File.dirname(__FILE__)))
  Warning.ignore(/gems\/(tzinfo|activesupport)-\d/)
  Warning.dedup if Warning.respond_to?(:dedup)
end
