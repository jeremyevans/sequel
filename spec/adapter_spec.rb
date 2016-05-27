if !ARGV.empty? && ARGV.first != 'none'
  require "./spec/adapters/#{ARGV.first}_spec.rb"
end
Dir['./spec/integration/*_test.rb'].each{|f| require f}
