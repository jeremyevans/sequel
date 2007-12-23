describe Sequel::Model, "hooks" do

  before do
    MODEL_DB.reset
    Sequel::Model.hooks.clear

    @hooks = %w[
      before_save before_create before_update before_destroy
      after_save after_create after_update after_destroy
    ].select { |hook| !hook.empty? }
  end

  it "should have hooks for everything" do
    Sequel::Model.methods.should include('hooks')
    Sequel::Model.methods.should include(*@hooks)
    @hooks.each do |hook|
      Sequel::Model.hooks[hook.to_sym].should be_an_instance_of(Array)
    end
  end

  it "should be inherited" do
    pending 'soon'

    @hooks.each do |hook|
      Sequel::Model.send(hook.to_sym) { nil }
    end

    model = Class.new Sequel::Model(:models)
    model.hooks.should == Sequel::Model.hooks
  end

  it "should run hooks" do
    pending 'soon'

    test = mock 'Test'
    test.should_receive(:run).exactly(@hooks.length)

    @hooks.each do |hook|
      Sequel::Model.send(hook.to_sym) { test.run }
    end

    model = Class.new Sequel::Model(:models)
    model.hooks.should == Sequel::Model.hooks

    model_instance = model.new
    @hooks.each { |hook| model_instance.run_hooks(hook) }
  end

  it "should run hooks around save and create" do
    pending 'test execution'
  end

  it "should run hooks around save and update" do
    pending 'test execution'
  end

  it "should run hooks around delete" do
    pending 'test execution'
  end

end

describe "Model.after_create" do

  before(:each) do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
      def columns
        [:id, :x, :y]
      end
    end

    ds = @c.dataset
    def ds.insert(*args)
      super(*args)
      1
    end
  end

  it "should be called after creation" do
    s = []

    @c.after_create do
      s = MODEL_DB.sqls.dup
    end

    n = @c.create(:x => 1)
    MODEL_DB.sqls.should == ["INSERT INTO items (x) VALUES (1)", "SELECT * FROM items WHERE (id = 1) LIMIT 1"]
    s.should == ["INSERT INTO items (x) VALUES (1)", "SELECT * FROM items WHERE (id = 1) LIMIT 1"]
  end

  it "should allow calling save in the hook" do
    @c.after_create do
      values.delete(:x)
      self.id = 2
      save
    end

    n = @c.create(:id => 1)
    MODEL_DB.sqls.should == ["INSERT INTO items (id) VALUES (1)", "SELECT * FROM items WHERE (id = 1) LIMIT 1", "UPDATE items SET id = 2 WHERE (id = 1)"]
  end

end

