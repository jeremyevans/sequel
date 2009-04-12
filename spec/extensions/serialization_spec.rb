require File.join(File.dirname(__FILE__), "spec_helper")

require 'yaml'

describe "Serialization plugin" do
  before do
    @c = Class.new(Sequel::Model(:items)) do
      include(Module.new do
        def before_save
        end
      end)
      no_primary_key
      columns :id, :abc, :def
    end
    MODEL_DB.reset
  end

  it "should allow serializing attributes to yaml" do
    @c.plugin :serialization, :yaml, :abc
    @c.create(:abc => 1)
    @c.create(:abc => "hello")

    MODEL_DB.sqls.should == [ \
      "INSERT INTO items (abc) VALUES ('--- 1\n')", \
      "INSERT INTO items (abc) VALUES ('--- hello\n')", \
    ]
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

  it "should translate values to and from yaml serialization format using accessor methods" do
    @c.set_primary_key :id
    @c.plugin :serialization, :yaml, :abc, :def
    vals = nil

    ds = @c.dataset
    def ds.fetch_rows(sql, &block)
      block.call(:id => 1, :abc => "--- 1\n", :def => "--- hello\n")
    end

    o = @c.first
    o.id.should == 1
    o.abc.should == 1
    o.def.should == "hello"

    o.update(:abc => 23)
    @c.create(:abc => [1, 2, 3])
    MODEL_DB.sqls.should == ["UPDATE items SET abc = '--- 23\n' WHERE (id = 1)",
      "INSERT INTO items (abc) VALUES ('#{[1, 2, 3].to_yaml}')"]
  end

  it "should translate values to and from marshal serialization format using accessor methods" do
    @c.set_primary_key :id
    @c.plugin :serialization, :marshal, :abc, :def

    ds = @c.dataset
    def ds.fetch_rows(sql, &block)
      block.call(:id => 1, :abc =>[Marshal.dump(1)].pack('m'), :def =>[Marshal.dump('hello')].pack('m'))
    end

    o = @c.first
    o.id.should == 1
    o.abc.should == 1
    o.def.should == "hello"

    o.update(:abc => 23)
    @c.create(:abc => [1, 2, 3])
    MODEL_DB.sqls.should == ["UPDATE items SET abc = '#{[Marshal.dump(23)].pack('m')}' WHERE (id = 1)",
      "INSERT INTO items (abc) VALUES ('#{[Marshal.dump([1, 2, 3])].pack('m')}')"]
  end

  it "should copy serialization formats and columns to subclasses" do
    @c.set_primary_key :id
    @c.plugin :serialization, :yaml, :abc, :def

    ds = @c.dataset
    def ds.fetch_rows(sql, &block)
      block.call(:id => 1, :abc => "--- 1\n", :def => "--- hello\n")
    end

    o = Class.new(@c).first
    o.id.should == 1
    o.abc.should == 1
    o.def.should == "hello"

    o.update(:abc => 23)
    Class.new(@c).create(:abc => [1, 2, 3])
    MODEL_DB.sqls.should == ["UPDATE items SET abc = '--- 23\n' WHERE (id = 1)",
      "INSERT INTO items (abc) VALUES ('#{[1, 2, 3].to_yaml}')"]
  end

  it "should clear the deserialized columns when refreshing" do
    @c.set_primary_key :id
    @c.plugin :serialization, :yaml, :abc, :def
    o = @c.load(:id => 1, :abc => "--- 1\n", :def => "--- hello\n")
    o.abc = 23
    o.deserialized_values.length.should == 1
    o.refresh
    o.deserialized_values.length.should == 0
  end
end
