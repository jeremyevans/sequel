require_relative "spec_helper"

describe "pg_auto_validate_enums plugin" do
  before do
    @db = Sequel.mock(:host=>'postgres')
    def @db.schema(table, *)
      table = table.first_source if table.is_a?(Sequel::Dataset)
      [[:c, {:enum_values=>["a", "b", table.to_s]}]]
    end
    @c = Sequel::Model(@db[:test])
    @c.plugin :pg_auto_validate_enums
    @o = @c.new
  end

  it "validates enum values automatically" do
    @o.c = "a"
    @o.valid?.must_equal true
    @o.c = "b"
    @o.valid?.must_equal true
    @o.c = "test"
    @o.valid?.must_equal true
    @o.c = "c"
    @o.valid?.must_equal false
    @o.errors.must_equal({:c => ['is not in range or set: ["a", "b", "test"]']})
  end

  it "handles case where no enum columns exist in model's table" do
    @db = Sequel.mock(:host=>'postgres')
    def @db.schema(table, *)
      table = table.first_source if table.is_a?(Sequel::Dataset)
      [[:c, {}]]
    end
    @c = Sequel::Model(@db[:test])
    @c.plugin :pg_auto_validate_enums
    @o = @c.new
    @o.c = "a"
    @o.valid?.must_equal true
  end

  it "validates underlying enum value, not method return value" do
    @o.c = "a"
    def @o.c; "invalid" end
    @o.valid?.must_equal true
  end

  it "treats plugin options as validates_includes options for validations" do
    @c.plugin :pg_auto_validate_enums, :message=>"is not valid"
    @o.c = "c"
    @o.valid?.must_equal false
    @o.errors.must_equal({:c => ['is not valid']})
  end

  it "reloads enum_values metadata when dataset is changed" do
    @c.dataset = :test2
    @o.c = "test2"
    @o.valid?.must_equal true
    @o.c = "test"
    @o.valid?.must_equal false
    @o.errors.must_equal({:c => ['is not in range or set: ["a", "b", "test2"]']})
  end

  it "works when subclassing" do
    c = Sequel::Model(@db)
    c.plugin :pg_auto_validate_enums
    sc = c::Model(:test2)
    o = sc.new
    o.c = "a"
    o.valid?.must_equal true
    o.c = "b"
    o.valid?.must_equal true
    o.c = "test2"
    o.valid?.must_equal true
    o.c = "c"
    o.valid?.must_equal false
    o.errors.must_equal({:c => ['is not in range or set: ["a", "b", "test2"]']})
  end

  it "exposes metadata in .pg_auto_validate_enums_metadata" do
    @c.pg_auto_validate_enums_metadata.must_equal(:c => ["a", "b", "test"])
  end

  it "exposes options in .pg_auto_validate_enums_opts" do
    @c.pg_auto_validate_enums_opts.must_equal(:allow_nil => true, :from => :values)
    @c.plugin :pg_auto_validate_enums, :message=>"is not valid"
    @c.pg_auto_validate_enums_opts.must_equal(:allow_nil => true, :message => "is not valid", :from => :values)
  end
end
