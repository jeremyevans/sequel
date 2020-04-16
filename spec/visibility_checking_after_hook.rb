require_relative "visibility_checking"
model_subclasses = []

[Sequel::Database, Sequel::Dataset, Sequel::Model, Sequel::Model.singleton_class].each do |c|
  VISIBILITY_CHANGES.concat(VisibilityChecker.visibility_changes(c).map{|v| [v, c.inspect]})
end

Sequel::Model.singleton_class.class_eval do
  prepend(Module.new do
    private
    define_method(:inherited) do |sc|
      model_subclasses << sc
      super(sc)
    end
  end)
end

Minitest::HooksSpec.class_eval do
  after do
    path, lineno = method(@NAME).source_location
    check = []
    Sequel::DATABASES.each do |db|
      check.push(db.singleton_class)
      check.push(db.dataset.singleton_class)
    end
    Sequel::DATABASES.clear

    subclasses = model_subclasses.dup
    model_subclasses.clear
    check.concat(subclasses)
    check.concat(subclasses.map(&:singleton_class))
    check.concat(subclasses.map{|c| c.dataset.singleton_class if c.instance_variable_get(:@dataset)})

    check.each do |c|
      next unless c
      VISIBILITY_CHANGES.concat(VisibilityChecker.visibility_changes(c).map{|v| [v, "#{path}:#{lineno}"]})
    end
  end
end

