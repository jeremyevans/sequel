require_relative "spec_helper"

describe "Sequel::Plugins::AutoValidations" do
  before do
    db = Sequel.mock(:fetch=>proc{|sql| sql =~ /a{51}/ ? {:v=>0} : {:v=>1}})
    def db.schema_parse_table(*) true; end
    def db.schema(t, *)
      t = t.first_source if t.is_a?(Sequel::Dataset)
      return [] if t != :test
      [[:id, {:primary_key=>true, :type=>:integer, :allow_null=>false}],
       [:name, {:primary_key=>false, :type=>:string, :allow_null=>false, :max_length=>50}],
       [:d, {:primary_key=>false, :type=>:date, :allow_null=>false}]]
    end
    def db.supports_index_parsing?() true end
    db.singleton_class.send(:alias_method, :supports_index_parsing?, :supports_index_parsing?)
    def db.indexes(t, *)
      raise if t.is_a?(Sequel::Dataset)
      return [] if t != :test
      {:a=>{:columns=>[:name, :num], :unique=>true}, :b=>{:columns=>[:num], :unique=>false}}
    end
    db.singleton_class.send(:alias_method, :indexes, :indexes)
    @c = Class.new(Sequel::Model(db[:test]))
    @c.send(:def_column_accessor, :id, :name, :num, :d, :nnd)
    @c.raise_on_typecast_failure = false
    @m = @c.new
    @c.db.sqls
    @c.db.fetch = {:table=>'test', :message=>'this is a bad column', :allow_nil=>true, :constraint_name=>nil, :validation_type=>'presence', :argument=>nil, :column=>'name'}
  end

  [true, false, nil].each do |before|
    [true, false].each do |set_dataset|
      it "should use constraint validations presence message if #{before ? 'constraint_validations' : 'auto_validations'} is loaded first#{' when using set_dataset' if set_dataset}" do
        if before
          @c.plugin :constraint_validations
          @c.plugin :auto_validations
        elsif !before.nil?
          @c.plugin :auto_validations
          @c.plugin :constraint_validations
        end
        @c.plugin :auto_validations_constraint_validations_presence_message

        if set_dataset
          @c.set_dataset :test
        end

        @c.db.sqls.must_equal ["SELECT * FROM sequel_constraint_validations"]
        @c.db.constraint_validations.must_equal("test"=>[{:allow_nil=>true, :constraint_name=>nil, :message=>'this is a bad column', :validation_type=>"presence", :column=>"name", :argument=>nil, :table=>"test"}])
        @c.constraint_validations.must_equal [[:validates_presence, :name, {:message=>'this is a bad column', :allow_nil=>false}]]
        @c.constraint_validation_reflections.must_equal(:name=>[[:presence, {:message=>'this is a bad column', :allow_nil=>true}]])
        @m.name = ''
        @m.valid?.must_equal false
        @m.errors.must_equal(:d=>["is not present"], :name=>["this is a bad column"])
        @m.name = nil
        @m.valid?.must_equal false
        @m.errors.must_equal(:d=>["is not present"], :name=>["this is a bad column"])
      end
    end
  end

  it "should not override auto_validations message if constraint_validations doesn't have a message" do
    @c.db.fetch = {:table=>'test', :message=>nil, :allow_nil=>true, :constraint_name=>nil, :validation_type=>'presence', :argument=>nil, :column=>'name'}
    @c.plugin :auto_validations_constraint_validations_presence_message
    @m.valid?.must_equal false
    @m.errors.must_equal(:d=>["is not present"], :name=>["is not present"])
  end

  it "should not override auto_validations message if constraint_validations does not have a message and does not allow nil values" do
    @c.db.fetch = {:table=>'test', :message=>nil, :allow_nil=>false, :constraint_name=>nil, :validation_type=>'presence', :argument=>nil, :column=>'name'}
    @c.plugin :auto_validations_constraint_validations_presence_message
    @m.valid?.must_equal false
    @m.errors.must_equal(:d=>["is not present"], :name=>["is not present", "is not present"])
  end

  it "should not override auto_validations message if constraint_validations does not allow nil values" do
    @c.db.fetch = {:table=>'test', :message=>'this is a bad column', :allow_nil=>false, :constraint_name=>nil, :validation_type=>'presence', :argument=>nil, :column=>'name'}
    @c.plugin :auto_validations_constraint_validations_presence_message
    @m.valid?.must_equal false
    @m.errors.must_equal(:d=>["is not present"], :name=>["is not present", "this is a bad column"])
  end

  it "should not override auto_validations message if auto_validations plugin uses a not_null message" do
    @c.plugin :auto_validations, :not_null_opts=>{:message=>'default'}
    @c.plugin :auto_validations_constraint_validations_presence_message
    @m.valid?.must_equal false
    @m.errors.must_equal(:d=>["default"], :name=>["default"])
  end

  it "should not override auto_validations message if auto_validations plugin uses an explicit_not_null message" do
    @c.db.singleton_class.send(:remove_method, :schema)
    def (@c.db).schema(t, *)
      t = t.first_source if t.is_a?(Sequel::Dataset)
      return [] if t != :test
      [[:id, {:primary_key=>true, :type=>:integer, :allow_null=>false}],
       [:name, {:primary_key=>false, :type=>:string, :allow_null=>false, :max_length=>50, :default=>'a'}],
       [:d, {:primary_key=>false, :type=>:date, :allow_null=>false, :default=>'2000-10-10'}]]
    end
    @c.set_dataset :test
    @c.plugin :auto_validations, :explicit_not_null_opts=>{:message=>'default'}
    @c.plugin :auto_validations_constraint_validations_presence_message
    @m.d = nil
    @m.name = nil
    @m.valid?.must_equal false
    @m.errors.must_equal(:d=>["default"], :name=>["default"])
  end

  it "should handle case where there isn't an NOT NULL constraint on the column" do
    @c.db.singleton_class.send(:remove_method, :schema)
    def (@c.db).schema(t, *)
      t = t.first_source if t.is_a?(Sequel::Dataset)
      return [] if t != :test
      [[:id, {:primary_key=>true, :type=>:integer, :allow_null=>false}],
       [:name, {:primary_key=>false, :type=>:string, :allow_null=>true, :max_length=>50, :default=>'a'}],
       [:d, {:primary_key=>false, :type=>:date, :allow_null=>true, :default=>'2000-10-10'}]]
    end
    @c.set_dataset :test
    @c.plugin :auto_validations_constraint_validations_presence_message
    @m.valid?.must_equal true
  end
end
