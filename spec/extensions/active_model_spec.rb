require File.join(File.dirname(__FILE__), "spec_helper")
if (begin
  require 'active_model'
  true
  rescue LoadError
  end)
describe "ActiveModel plugin" do
  before do
    @c = Class.new(Sequel::Model) do
      def delete; end
    end
    @c.plugin :active_model
    @m = @c.new
    @o = @c.load({})
  end

  specify "should be compliant to the ActiveModel spec" do
    s = ''
    IO.popen('-') do |f|
      if f
        s = f.read
      else
        require 'test/unit'
        require "test/unit/ui/console/testrunner"
        $c = @c
        class AMLintTest < Test::Unit::TestCase
          def setup
            @model = $c.new
          end
          include ActiveModel::Lint::Tests
        end
        Test::Unit::UI::Console::TestRunner.run(AMLintTest)
      end
    end
    s.should =~ /0 failures, 0 errors/
  end
  
  specify "to_model should return self" do
    @m.to_model.object_id.should == @m.object_id
  end
  
  specify "new_record? should be aliased to new" do
    @m.new_record?.should == true
    @o.new_record?.should == false
  end
  
  specify "new_record? should be aliased to new" do
    @m.destroyed?.should == false
    @o.destroyed?.should == false
    @m.destroy
    @o.destroy
    @m.destroyed?.should == true
    @o.destroyed?.should == true
  end
end 
end
