if RUBY_VERSION >= '3' && [:mysql, :mysql2].include?(DB.adapter_scheme)
  begin
    require 'warning'
  rescue LoadError
  else
    Warning.ignore(:taint)
    Warning.dedup if Warning.respond_to?(:dedup)
  end
end
