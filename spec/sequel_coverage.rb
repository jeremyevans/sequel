require 'simplecov'

def SimpleCov.sequel_coverage(opts = {})
  start do
    enable_coverage :branch
    command_name SEQUEL_COVERAGE unless SEQUEL_COVERAGE == "1"

    add_filter "/spec/"
    add_group('Missing-Revelent'){|src| src.filename =~ opts[:group] && src.covered_percent < 100} if opts[:group]
    add_group('Missing'){|src| src.covered_percent < 100}
    add_group('Covered'){|src| src.covered_percent == 100}

    if ENV['SEQUEL_MERGE_COVERAGE']
      filter = %r{bin/sequel\z|lib/sequel/(\w+\.rb|(dataset|database|model|connection_pool|extensions|plugins)/\w+\.rb|adapters/mock\.rb)\z}
      add_filter{|src| src.filename !~ filter}
    elsif opts[:filter]
      add_filter{|src| src.filename !~ opts[:filter]}
    end

    if opts[:subprocesses]
      enable_for_subprocesses true
      ENV['COVERAGE'] = 'subprocess'
      ENV['RUBYOPT'] = "#{ENV['RUBYOPT']} -r ./spec/sequel_coverage"
    elsif SEQUEL_COVERAGE == 'subprocess'
      command_name "bin-#{$$}"
      self.print_error_status = false
      formatter SimpleCov::Formatter::SimpleFormatter
    end
  end
end

SEQUEL_COVERAGE = ENV.delete('COVERAGE')

if SEQUEL_COVERAGE == 'subprocess'
  SimpleCov.sequel_coverage
end
