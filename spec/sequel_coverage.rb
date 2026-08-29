# frozen_string_literal: true
require 'simplecov'

def SimpleCov.sequel_coverage(opts = {})
  start do
    command_name SEQUEL_COVERAGE unless SEQUEL_COVERAGE == "1"
    coverage :line
    coverage :branch
    cover "lib/**/*.rb", "bin/sequel"
    group('Missing'){|src| src.covered_percent < 100}

    if ENV['SEQUEL_MERGE_COVERAGE']
      filter = %r{bin/sequel\z|lib/sequel/(\w+\.rb|(dataset|database|model|connection_pool|extensions|plugins)/\w+\.rb|adapters/(mock|(shared/)?postgres)\.rb)\z}
      exclude = %r{lib/sequel/(extensions/(from_block|mssql_emulate_lateral_with_apply|no_auto_literal_strings)|plugins/before_after_save).rb}
      skip{|src| src.filename !~ filter || src.filename =~ exclude}
      merge_timeout 600
    elsif opts[:filter]
      skip{|src| src.filename !~ opts[:filter]}
    end

    if opts[:subprocesses]
      merge_subprocesses true
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
