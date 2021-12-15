require 'coverage'
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
      regexps = [%r{lib/sequel/(extensions|plugins)/\w+\.rb\z}, %r{lib/sequel/(\w+\.rb|(dataset|database|model|connection_pool)/\w+\.rb|adapters/mock\.rb)\z}]
      add_filter{|src| src.filename !~ Regexp.union(regexps)}
    else
      add_filter{|src| src.filename !~ opts[:filter]} if opts[:filter]
    end
  end
end

SEQUEL_COVERAGE = ENV.delete('COVERAGE')
