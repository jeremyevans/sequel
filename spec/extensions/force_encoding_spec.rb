require File.join(File.dirname(__FILE__), "spec_helper")
if RUBY_VERSION >= '1.9.0'
describe "force_encoding plugin" do
  before do
    @c = Class.new(Sequel::Model) do
    end
    @c.columns :id, :x
    @c.plugin :force_encoding, 'UTF-8'
    @e1 = Encoding.find('UTF-8')
  end

  specify "should force encoding to given encoding on load" do
    s = 'blah'
    s.force_encoding('US-ASCII')
    o = @c.load(:id=>1, :x=>s)
    o.x.should == 'blah'
    o.x.encoding.should == @e1
  end
  
  specify "should force encoding to given encoding when setting column values" do
    s = 'blah'
    s.force_encoding('US-ASCII')
    o = @c.new(:x=>s)
    o.x.should == 'blah'
    o.x.encoding.should == @e1
  end
  
  specify "should have a forced_encoding class accessor" do
    s = 'blah'
    s.force_encoding('US-ASCII')
    @c.forced_encoding = 'Windows-1258'
    o = @c.load(:id=>1, :x=>s)
    o.x.should == 'blah'
    o.x.encoding.should == Encoding.find('Windows-1258')
  end
  
  specify "should not force encoding if forced_encoding is nil" do
    s = 'blah'
    s.force_encoding('US-ASCII')
    @c.forced_encoding = nil
    o = @c.load(:id=>1, :x=>s)
    o.x.should == 'blah'
    o.x.encoding.should == Encoding.find('US-ASCII')
  end
  
  specify "should work correctly when subclassing" do
    c = Class.new(@c)
    s = 'blah'
    s.force_encoding('US-ASCII')
    o = c.load(:id=>1, :x=>s)
    o.x.should == 'blah'
    o.x.encoding.should == @e1
    
    c.plugin :force_encoding, 'UTF-16LE'
    s = ''
    s.force_encoding('US-ASCII')
    o = c.load(:id=>1, :x=>s)
    o.x.should == ''
    o.x.encoding.should == Encoding.find('UTF-16LE')
    
    @c.plugin :force_encoding, 'UTF-32LE'
    s = ''
    s.force_encoding('US-ASCII')
    o = @c.load(:id=>1, :x=>s)
    o.x.should == ''
    o.x.encoding.should == Encoding.find('UTF-32LE')
    
    s = ''
    s.force_encoding('US-ASCII')
    o = c.load(:id=>1, :x=>s)
    o.x.should == ''
    o.x.encoding.should == Encoding.find('UTF-16LE')
  end
end 
end