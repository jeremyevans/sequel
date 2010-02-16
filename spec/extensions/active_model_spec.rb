require File.join(File.dirname(__FILE__), "spec_helper")
describe "ActiveModel plugin" do
  specify "should be compliant to the ActiveModel spec" do
    s = ''
    IO.popen('-') do |f|
      if f
        s = f.read
      else
        require 'active_model'
        require 'test/unit'
        require "test/unit/ui/console/testrunner"
        class AMLintTest < Test::Unit::TestCase
          def setup
            @c = Class.new(Sequel::Model) do
              def delete; end
            end
            @c.plugin :active_model
            @m = @model = @c.new
            @o = @c.load({})
          end
          include ActiveModel::Lint::Tests

          def test_to_model
            assert_equal @m.to_model.object_id.should, @m.object_id
          end

          def test_new_record
            assert_equal true, @m.new_record?
            assert_equal false, @o.new_record?
          end

          def test_destroyed
            assert_equal false, @m.destroyed?
            assert_equal false, @o.destroyed?
            @m.destroy
            @o.destroy
            assert_equal true, @m.destroyed?
            assert_equal true, @o.destroyed?
          end
        end
        Test::Unit::UI::Console::TestRunner.run(AMLintTest)
      end
    end
    s.should =~ /0 failures, 0 errors/
  end
end 
