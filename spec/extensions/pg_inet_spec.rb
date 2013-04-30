require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "pg_inet extension" do
  ipv6_broken = (IPAddr.new('::1'); false) rescue true
  before do
    @db = Sequel.connect('mock://postgres', :quote_identifiers=>false)
    @db.extension(:pg_array, :pg_inet)
  end

  it "should literalize IPAddr v4 instances to strings correctly" do
    @db.literal(IPAddr.new('127.0.0.1')).should == "'127.0.0.1/32'"
    @db.literal(IPAddr.new('127.0.0.0/8')).should == "'127.0.0.0/8'"
  end

  it "should literalize IPAddr v6 instances to strings correctly" do
    @db.literal(IPAddr.new('2001:4f8:3:ba::/64')).should == "'2001:4f8:3:ba::/64'"
    @db.literal(IPAddr.new('2001:4f8:3:ba:2e0:81ff:fe22:d1f1')).should == "'2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128'"
  end unless ipv6_broken

  it "should not affect literalization of custom objects" do
    o = Object.new
    def o.sql_literal(ds) 'v' end
    @db.literal(o).should == 'v'
  end

  it "should support using IPAddr as bound variables" do
    @db.bound_variable_arg(1, nil).should == 1
    @db.bound_variable_arg(IPAddr.new('127.0.0.1'), nil).should == '127.0.0.1/32'
  end

  it "should support using IPAddr instances in array types in bound variables" do
    @db.bound_variable_arg(Sequel.pg_array([IPAddr.new('127.0.0.1')]), nil).should == '{"127.0.0.1/32"}'
  end

  it "should parse inet/cidr type from the schema correctly" do
    @db.fetch = [{:name=>'id', :db_type=>'integer'}, {:name=>'i', :db_type=>'inet'}, {:name=>'c', :db_type=>'cidr'}]
    @db.schema(:items).map{|e| e[1][:type]}.should == [:integer, :ipaddr, :ipaddr]
  end

  it "should support typecasting for the ipaddr type" do
    ip = IPAddr.new('127.0.0.1')
    @db.typecast_value(:ipaddr, ip).should equal(ip)
    @db.typecast_value(:ipaddr, ip.to_s).should == ip
    proc{@db.typecast_value(:ipaddr, '')}.should raise_error(Sequel::InvalidValue)
    proc{@db.typecast_value(:ipaddr, 1)}.should raise_error(Sequel::InvalidValue)
  end

  it "should return correct results for Database#schema_type_class" do
    @db.schema_type_class(:ipaddr).should == IPAddr
    @db.schema_type_class(:integer).should == Integer
  end
end
