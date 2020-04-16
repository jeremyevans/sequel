require 'visibility_checker'
changes = VISIBILITY_CHANGES = []
Minitest.after_run do
  if defined?(DB)
    [DB.singleton_class, DB.dataset.singleton_class].each do |c|
      VISIBILITY_CHANGES.concat(VisibilityChecker.visibility_changes(c).map{|v| [v, c.inspect]})
    end
  end

  changes.uniq!{|v,| v}
  changes.map! do |v, caller|
    "#{caller}: #{v.new_visibility} method #{v.overridden_by}##{v.method} overrides #{v.original_visibility} method in #{v.defined_in}"
  end
  changes.sort!
  if changes.empty?
    puts "No visibility changes"
  else
    puts "Visibility changes:"
    puts(*changes)
  end
end
