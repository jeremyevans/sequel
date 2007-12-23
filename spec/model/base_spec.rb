require File.join(File.dirname(__FILE__), "../spec_helper")

Sequel::Model.db = MODEL_DB = MockDatabase.new

describe "Model attribute setters" do

  before(:each) do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
      def columns
        [:id, :x, :y]
      end
    end
  end

  it "should mark the column value as changed" do
    o = @c.new
    o.changed_columns.should == []

    o.x = 2
    o.changed_columns.should == [:x]

    o.y = 3
    o.changed_columns.should == [:x, :y]

    o.changed_columns.clear

    o[:x] = 2
    o.changed_columns.should == [:x]

    o[:y] = 3
    o.changed_columns.should == [:x, :y]
  end

end

describe "Model#serialize" do

  before(:each) do
    MODEL_DB.reset
  end

  it "should translate values to YAML when creating records" do
    @c = Class.new(Sequel::Model(:items)) do
      no_primary_key
      serialize :abc
    end

    @c.create(:abc => 1)
    @c.create(:abc => "hello")

    MODEL_DB.sqls.should == [ \
      "INSERT INTO items (abc) VALUES ('--- 1\n')", \
      "INSERT INTO items (abc) VALUES ('--- hello\n')", \
    ]
  end

  it "should support calling after the class is defined" do
    @c = Class.new(Sequel::Model(:items)) do
      no_primary_key
    end

    @c.serialize :def

    @c.create(:def => 1)
    @c.create(:def => "hello")

    MODEL_DB.sqls.should == [ \
      "INSERT INTO items (def) VALUES ('--- 1\n')", \
      "INSERT INTO items (def) VALUES ('--- hello\n')", \
    ]
  end

  it "should support using the Marshal format" do
    @c = Class.new(Sequel::Model(:items)) do
      no_primary_key
      serialize :abc, :format => :marshal
    end

    @c.create(:abc => 1)
    @c.create(:abc => "hello")

    MODEL_DB.sqls.should == [ \
      "INSERT INTO items (abc) VALUES ('\004\bi\006')", \
      "INSERT INTO items (abc) VALUES ('\004\b\"\nhello')", \
    ]
  end

  it "should translate values to and from YAML using accessor methods" do
    @c = Class.new(Sequel::Model(:items)) do
      serialize :abc, :def
    end

    ds = @c.dataset
    ds.extend(Module.new {
      attr_accessor :raw

      def fetch_rows(sql, &block)
        block.call(@raw)
      end

      @@sqls = nil

      def insert(*args)
        @@sqls = insert_sql(*args)
      end

      def update(*args)
        @@sqls = update_sql(*args)
      end

      def sqls
        @@sqls
      end

      def columns
        [:id, :abc, :def]
      end
    }
    )

    ds.raw = {:id => 1, :abc => "--- 1\n", :def => "--- hello\n"}
    o = @c.first
    o.id.should == 1
    o.abc.should == 1
    o.def.should == "hello"

    o.set(:abc => 23)
    ds.sqls.should == "UPDATE items SET abc = '#{23.to_yaml}' WHERE (id = 1)"

    ds.raw = {:id => 1, :abc => "--- 1\n", :def => "--- hello\n"}
    o = @c.create(:abc => [1, 2, 3])
    ds.sqls.should == "INSERT INTO items (abc) VALUES ('#{[1, 2, 3].to_yaml}')"
  end

end

describe Sequel::Model, "super_dataset" do
  
  before(:each) do
    MODEL_DB.reset
    class SubClass < Sequel::Model(:items) ; end
  end
  
  it "should call the superclass's dataset" do
    SubClass.should_receive(:superclass).exactly(3).times.and_return(Sequel::Model(:items))
    Sequel::Model(:items).should_receive(:dataset)
    SubClass.super_dataset
  end

end