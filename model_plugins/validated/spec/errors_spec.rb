require "#{ File.dirname(__FILE__) }/spec_helper.rb"

describe subject::Errors do
  
  before(:each) { @errors = subject::Errors.new }
  
  it "should add errors in a hash" do
    @errors.add :attribute, 'Message'

    @errors.instance_variable_get(:@errors)[:attribute].
    should include('Message')
  end
  it "should return full messages" do
    @errors.add :attribute, 'Message #{"%s".capitalize}'

    @errors.full_messages.
    should == ['Message Attribute']
  end
  it "should return an #{subject::ValidationException} with errors" do
    exception = @errors.to_exception
    exception.should be_an_instance_of(subject::ValidationException)
    exception.errors.should == @errors
  end
  it "should have methods delegated" do
    probe = mock 'Probe'
    methods = [:empty?, :clear, :[], :each, :to_yaml]
    
    methods.each { |m| probe.should_receive m }
    @errors.instance_variable_set :@errors, probe
    
    methods.each { |m| @errors.send! m }
  end
  it "should return evaluated errors messages" do
    @errors.add :attribute, 'Message #{"%s".capitalize}'
    @errors.on(:attribute).should == ['Message Attribute']
  end
  
end

describe subject::ValidationException do
  
  it "should return an exception with errors" do
    probe = mock 'probe'
    probe.should_receive(:any?).and_return(true)
    exception = subject::ValidationException.new probe

    lambda { raise exception }.should_not raise_error(TypeError)
    exception.errors.should == probe
  end
  it "should have methods delegated" do
    methods = [:on, :full_messages, :each]

    probe = mock 'Probe'
    probe.should_receive(:any?).and_return(true)
    exception = subject::ValidationException.new probe

    methods.each { |m| probe.should_receive m }
    methods.each { |m| exception.send! m }
  end
  
end