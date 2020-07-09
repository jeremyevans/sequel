require 'coverage'
require 'simplecov'

def SimpleCov.sequel_coverage(opts = {})
  start do
    enable_coverage :branch
    command_name SEQUEL_COVERAGE unless SEQUEL_COVERAGE == "1"

    # Work around Ruby Bug #16967
    Coverage.singleton_class.prepend(Module.new do
      def result
        res = super
        check_branch = true
        skip_2nd = lambda do |ary|
          ary = ary.dup
          ary.slice!(1)
          ary
        end
        res.values.each do |hash|
          if check_branch
            unless hash.is_a?(Hash) && hash[:branches]
              return res
            end
            check_branch = false
          end
          unique_branches = {}
          branch_counters = {}
          new_branches = {}
          branches = hash[:branches]
          branches.each do |k, v|
            new_k = skip_2nd[k]
            if branch_values = unique_branches[new_k]
              v.each do |k1, v1|
                branch_counters[skip_2nd[k1]] += v1
              end
              branch_values.keys.each do |k1|
                branch_values[k1] = branch_counters[skip_2nd[k1]]
              end
            else
              unique_branches[new_k] = new_branches[k] = v
              v.each do |k1, v1|
                branch_counters[skip_2nd[k1]] = v1
              end
            end
          end
          hash[:branches] = new_branches
        end
        res
      end
    end)

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
