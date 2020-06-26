require 'coverage'
require 'simplecov'

def SimpleCov.sequel_coverage(opts = {})
  start do
    enable_coverage :branch

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
    add_filter{|src| src.filename !~ opts[:filter]} if opts[:filter]
    yield self if block_given?
  end
end

ENV.delete('COVERAGE')
