require "#{ File.dirname(__FILE__) }/spec_helper.rb"

describe subject::Validation do
  
  it "should register validations if inherited" do
    subject::Builder.
      should_receive(:update).any_number_of_times.
      with Class.new(subject::Validation)
    pending 'This one kinda sucks...'
  end
  it "should build validations with block" do
    block = proc {|o, a, v|}
    validation = subject::Validation.new({}, &block)
    
    validation.instance_variable_get(:@block).should == block
  end
  it "should alias call to call_without_conditions" do
    validation = subject::Validation.new({}) {|o, a, v|}
    
    validation.method(:call).
    should == validation.method(:call_without_conditions)
  end
  it "should not build conditions" do
    validation = subject::Validation.
    new({:if => nil, :unless => nil}) {|o, a, v|}

    validation.instance_variable_get(:@conditions).should be_empty
  end
  it "should build conditions" do
    validation = subject::Validation.new({:if => :nil?}) {|o, a, v|}
    validation.instance_variable_get(:@conditions).should_not be_empty

    validation.instance_variable_get(:@conditions).
    first.instance_variable_get(:@condition).
    should == :nil?
  end
  it "should alias call to call_with_conditions" do
    validation = subject::Validation.new({:if => :nil?}) {|o, a, v|}
    
    validation.method(:call).
    should == validation.method(:call_with_conditions)
  end
  it "should call" do
    probe = mock 'Probe'
    probe.should_receive(:test).exactly(3).times
    
    validation = subject::Validation.
      new({}) { |o, a, v| [o, a, v].each { |p| p.test } }
    
    validation.call probe, probe, probe
  end
  it "should call unless a condition passes" do
    probe = mock 'Probe'
    probe.stub!(:nil?).and_return(true)
    
    validation = subject::Validation.
      new({:unless => :nil?}) { |o, a, v| [o, a, v].each { |p| p.test } }
    
    validation.call probe, probe, probe
    
    probe.should_receive(:test).exactly(3).times
    probe.stub!(:nil?).and_return(false)
    
    validation.call probe, probe, probe
  end
  it "should not call" do
    probe = mock 'Probe'
    probe.should_not_receive(:test)
    
    validation = subject::Validation.
      new({:if => :nil?}) { |o, a, v| [o, a, v].each { |p| p.test } }
    
    validation.call probe, probe, probe

  end
  it "should have validated attributes accessable" do
    validation = subject::Validation.new(:probe)
    validation.attributes.should include(:probe)
  end
  it "should clone observer peers to descendant" do
    peers = subject::Validation.instance_variable_get :@observer_peers
    peers.should_receive(:clone).and_return(true)
    descendant = Class.new(subject::Validation)
    descendant.instance_variable_get(:@observer_peers).should be_true
  end
  
end

describe subject::Validation::Condition do

  it "should evaluate positive callable" do
    condition = subject::Validation::Condition.new proc {|o| o.nil?}
    condition.evaluate(nil).should be_true
  end
  it "should evaluate negative callable" do
    condition = subject::Validation::Condition.new proc {|o| o.nil?}, false
    condition.evaluate(nil).should be_false
  end
  it "should evaluate positive Symbol" do
    condition = subject::Validation::Condition.new :nil?
    condition.evaluate(nil).should be_true
  end
  it "should evaluate negative Symbol" do
    condition = subject::Validation::Condition.new :nil?, false
    condition.evaluate(nil).should be_false
  end
  it "should evaluate positive UnboundMethod" do
    um = NilClass.instance_method :nil?
    condition = subject::Validation::Condition.new um
    condition.evaluate(nil).should be_true
  end
  it "should evaluate negative UnboundMethod" do
    um = NilClass.instance_method :nil?
    condition = subject::Validation::Condition.new um, false
    condition.evaluate(nil).should be_false
  end

end
