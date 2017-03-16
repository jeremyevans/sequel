Sequel::Deprecation.backtrace_filter = lambda{|line, lineno| lineno < 4 || line =~ /_spec\.rb/}

class Minitest::HooksSpec
  def self.deprecated(*a, &block)
    it(*a) do
      deprecated{instance_exec(&block)}
    end
  end

  def deprecated
    output = Sequel::Deprecation.output
    Sequel::Deprecation.output = nil
    yield
  ensure
    Sequel::Deprecation.output = output
  end
end
