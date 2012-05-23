require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

begin
require 'yaml'
require 'json'
rescue LoadError => e
  skip_warn "serialization plugin: can't load json (#{e.class}: #{e})"
else
describe "Serialization plugin" do
  before do
    @c = Class.new(Sequel::Model(:items)) do
      no_primary_key
      columns :id, :abc, :def, :ghi
    end
    MODEL_DB.reset
  end
  
  it "should allow setting additional serializable attributes via plugin :serialization call" do
    @c.plugin :serialization, :yaml, :abc
    @c.create(:abc => 1, :def=> 2)
    MODEL_DB.sqls.last.should =~ /INSERT INTO items \((abc, def|def, abc)\) VALUES \(('--- 1\n(\.\.\.\n)?', 2|2, '--- 1\n(\.\.\.\n)?')\)/

    @c.plugin :serialization, :marshal, :def
    @c.create(:abc => 1, :def=> 1)
    MODEL_DB.sqls.last.should =~ /INSERT INTO items \((abc, def|def, abc)\) VALUES \(('--- 1\n(\.\.\.\n)?', 'BAhpBg==\n'|'BAhpBg==\n', '--- 1\n(\.\.\.\n)?')\)/
    
    @c.plugin :serialization, :json, :ghi
    @c.create(:ghi => [123])
    MODEL_DB.sqls.last.should =~ /INSERT INTO items \((ghi)\) VALUES \('\[123\]'\)/
  end

  it "should allow serializing attributes to yaml" do
    @c.plugin :serialization, :yaml, :abc
    @c.create(:abc => 1)
    @c.create(:abc => "hello")

    MODEL_DB.sqls.map{|s| s.sub("...\n", '')}.should == ["INSERT INTO items (abc) VALUES ('--- 1\n')", "INSERT INTO items (abc) VALUES ('--- hello\n')"]
  end

  it "serialized_columns should be the columns serialized" do
    @c.plugin :serialization, :yaml, :abc
    @c.serialized_columns.should == [:abc]
  end

  it "should allow serializing attributes to marshal" do
    @c.plugin :serialization, :marshal, :abc
    @c.create(:abc => 1)
    @c.create(:abc => "hello")
    x = [Marshal.dump("hello")].pack('m')

    MODEL_DB.sqls.should == [ \
      "INSERT INTO items (abc) VALUES ('BAhpBg==\n')", \
      "INSERT INTO items (abc) VALUES ('#{x}')", \
    ]
  end
  
  it "should allow serializing attributes to json" do
    @c.plugin :serialization, :json, :ghi
    @c.create(:ghi => [1])
    @c.create(:ghi => ["hello"])
    
    x = ["hello"].to_json
    MODEL_DB.sqls.should == [ \
      "INSERT INTO items (ghi) VALUES ('[1]')", \
      "INSERT INTO items (ghi) VALUES ('#{x}')", \
    ]
  end

  it "should allow serializing attributes using arbitrary callable" do
    @c.plugin :serialization, [proc{|s| s.reverse}, proc{}], :abc
    @c.create(:abc => "hello")
    MODEL_DB.sqls.should == ["INSERT INTO items (abc) VALUES ('olleh')"]
  end
  
  it "should raise an error if specificing serializer as an unregistered symbol" do
    proc{@c.plugin :serialization, :foo, :abc}.should raise_error(Sequel::Error)
  end
  
  it "should translate values to and from yaml serialization format using accessor methods" do
    @c.set_primary_key :id
    @c.plugin :serialization, :yaml, :abc, :def
    vals = nil
    @c.dataset._fetch = {:id => 1, :abc => "--- 1\n", :def => "--- hello\n"}

    o = @c.first
    o.id.should == 1
    o.abc.should == 1
    o.abc.should == 1
    o.def.should == "hello"
    o.def.should == "hello"

    o.update(:abc => 23)
    @c.create(:abc => [1, 2, 3])
    MODEL_DB.sqls.should == ["SELECT * FROM items LIMIT 1",
      "UPDATE items SET abc = '#{23.to_yaml}' WHERE (id = 1)",
      "INSERT INTO items (abc) VALUES ('#{[1, 2, 3].to_yaml}')",
      "SELECT * FROM items WHERE (id = 10) LIMIT 1"]
  end

  it "should translate values to and from marshal serialization format using accessor methods" do
    @c.set_primary_key :id
    @c.plugin :serialization, :marshal, :abc, :def
    @c.dataset._fetch = [:id => 1, :abc =>[Marshal.dump(1)].pack('m'), :def =>[Marshal.dump('hello')].pack('m')]

    o = @c.first
    o.id.should == 1
    o.abc.should == 1
    o.abc.should == 1
    o.def.should == "hello"
    o.def.should == "hello"

    o.update(:abc => 23)
    @c.create(:abc => [1, 2, 3])
    MODEL_DB.sqls.should == ["SELECT * FROM items LIMIT 1",
      "UPDATE items SET abc = '#{[Marshal.dump(23)].pack('m')}' WHERE (id = 1)",
      "INSERT INTO items (abc) VALUES ('#{[Marshal.dump([1, 2, 3])].pack('m')}')",
      "SELECT * FROM items WHERE (id = 10) LIMIT 1"]
  end
  
  it "should translate values to and from json serialization format using accessor methods" do
    @c.set_primary_key :id
    @c.plugin :serialization, :json, :abc, :def
    @c.dataset._fetch = {:id => 1, :abc => [1].to_json, :def => ["hello"].to_json}
    
    o = @c.first
    o.id.should == 1
    o.abc.should == [1]
    o.abc.should == [1]
    o.def.should == ["hello"]
    o.def.should == ["hello"]
    
    o.update(:abc => [23])
    @c.create(:abc => [1,2,3])
    
    MODEL_DB.sqls.should == ["SELECT * FROM items LIMIT 1",
      "UPDATE items SET abc = '#{[23].to_json}' WHERE (id = 1)",
      "INSERT INTO items (abc) VALUES ('#{[1,2,3].to_json}')",
      "SELECT * FROM items WHERE (id = 10) LIMIT 1"]
  end

  it "should translate values to and from arbitrary callables using accessor methods" do
    @c.set_primary_key :id
    @c.plugin :serialization, [proc{|s| s.reverse}, proc{|s| s.reverse}], :abc, :def
    @c.dataset._fetch = {:id => 1, :abc => 'cba', :def => 'olleh'}
    
    o = @c.first
    o.id.should == 1
    o.abc.should == 'abc'
    o.abc.should == 'abc'
    o.def.should == "hello"
    o.def.should == "hello"
    
    o.update(:abc => 'foo')
    @c.create(:abc => 'bar')
    
    MODEL_DB.sqls.should == ["SELECT * FROM items LIMIT 1",
      "UPDATE items SET abc = 'oof' WHERE (id = 1)",
      "INSERT INTO items (abc) VALUES ('rab')",
      "SELECT * FROM items WHERE (id = 10) LIMIT 1"]
  end

  it "should handle registration of custom serializer/deserializer pairs" do
    @c.set_primary_key :id
    Sequel::Plugins::Serialization.register_format(:reverse, proc{|s| s.reverse}, proc{|s| s.reverse})
    @c.plugin :serialization, :reverse, :abc, :def
    @c.dataset._fetch = {:id => 1, :abc => 'cba', :def => 'olleh'}
    
    o = @c.first
    o.id.should == 1
    o.abc.should == 'abc'
    o.abc.should == 'abc'
    o.def.should == "hello"
    o.def.should == "hello"
    
    o.update(:abc => 'foo')
    @c.create(:abc => 'bar')
    
    MODEL_DB.sqls.should == ["SELECT * FROM items LIMIT 1",
      "UPDATE items SET abc = 'oof' WHERE (id = 1)",
      "INSERT INTO items (abc) VALUES ('rab')",
      "SELECT * FROM items WHERE (id = 10) LIMIT 1"]
  end

  it "should copy serialization formats and columns to subclasses" do
    @c.set_primary_key :id
    @c.plugin :serialization, :yaml, :abc, :def
    @c.dataset._fetch = {:id => 1, :abc => "--- 1\n", :def => "--- hello\n"}

    o = Class.new(@c).first
    o.id.should == 1
    o.abc.should == 1
    o.abc.should == 1
    o.def.should == "hello"
    o.def.should == "hello"

    o.update(:abc => 23)
    Class.new(@c).create(:abc => [1, 2, 3])
    MODEL_DB.sqls.should == ["SELECT * FROM items LIMIT 1",
      "UPDATE items SET abc = '#{23.to_yaml}' WHERE (id = 1)",
      "INSERT INTO items (abc) VALUES ('#{[1, 2, 3].to_yaml}')",
      "SELECT * FROM items WHERE (id = 10) LIMIT 1"]
  end

  it "should clear the deserialized columns when refreshing" do
    @c.set_primary_key :id
    @c.plugin :serialization, :yaml, :abc, :def
    o = @c.load(:id => 1, :abc => "--- 1\n", :def => "--- hello\n")
    o.abc = 23
    o.deserialized_values.length.should == 1
    o.abc.should == 23
    o.refresh
    o.deserialized_values.length.should == 0
  end
  
  it "should clear the deserialized columns when refreshing after saving a new object" do
    @c.set_primary_key :id
    @c.plugin :serialization, :yaml, :abc, :def
    o = @c.new(:abc => "--- 1\n", :def => "--- hello\n")
    o.deserialized_values.length.should == 2
    o.save
    o.deserialized_values.length.should == 0
  end
  
  it "should raise an error if calling internal serialization methods with bad columns" do
    @c.set_primary_key :id
    @c.plugin :serialization
    o = @c.load(:id => 1, :abc => "--- 1\n", :def => "--- hello\n")
    lambda{o.send(:serialize_value, :abc, 1)}.should raise_error(Sequel::Error)
    lambda{o.send(:deserialize_value, :abc, "--- hello\n")}.should raise_error(Sequel::Error)
  end

  it "should add the accessors to a module included in the class, so they can be easily overridden" do
    @c.class_eval do
      def abc
        "#{super}-blah"
      end
    end
    @c.plugin :serialization, :yaml, :abc
    o = @c.load(:abc => "--- 1\n")
    o.abc.should == "1-blah"
  end

  it "should call super to get the deserialized value from a previous accessor" do
    m = Module.new do
      def abc
        "--- #{@values[:abc]*3}\n"
      end
    end
    @c.send(:include, m)
    @c.plugin :serialization, :yaml, :abc
    o = @c.load(:abc => 3)
    o.abc.should == 9
  end
end
end
