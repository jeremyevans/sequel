require File.join(File.dirname(__FILE__), "spec_helper")
if RUBY_PLATFORM !~ /(win|w)32|java$/
describe "ActiveModel plugin" do
  specify "should be compliant to the ActiveModel spec" do
    s = ''
    IO.popen('-') do |f|
      if f
        s = f.read
      else
        begin
          require 'rubygems'
          require 'active_model'
        rescue LoadError
          puts "0 failures, 0 errors, skipping tests"
        else
          require 'test/unit'
          require "test/unit/ui/console/testrunner"
          class AMLintTest < Test::Unit::TestCase
            def setup
              @c = Class.new(Sequel::Model) do
                set_primary_key :id
                columns :id, :id2
                def delete; end
              end
              @c.plugin :active_model
              @m = @model = @c.new
              @o = @c.load({})
            end
            include ActiveModel::Lint::Tests

            # Should return self, not a proxy object
            def test__to_model
              assert_equal @m.to_model.object_id.should, @m.object_id
            end
            
            def test__to_key
              assert_equal nil, @m.to_key
              @o.id = 1
              assert_equal [1], @o.to_key
              @c.set_primary_key [:id2, :id]
              @o.id2 = 2
              assert_equal [2, 1], @o.to_key
              @o.destroy
              assert_equal nil, @o.to_key
            end
            
            def test__to_param
              assert_equal nil, @m.to_param
              @o.id = 1
              assert_equal '1', @o.to_param
              @c.set_primary_key [:id2, :id]
              @o.id2 = 2
              assert_equal '2-1', @o.to_param
              @o.meta_def(:to_param_joiner){'|'}
              assert_equal '2|1', @o.to_param
              @o.destroy
              assert_equal nil, @o.to_param
            end

            def test__persisted?
              assert_equal false, @m.persisted?
              assert_equal true, @o.persisted?
              @m.destroy
              @o.destroy
              assert_equal false, @m.persisted?
              assert_equal false, @o.persisted?
            end
          end
          Test::Unit::UI::Console::TestRunner.run(AMLintTest)
        end
      end
    end
    s.should =~ /0 failures, 0 errors/
  end
end 
else
  skip_warn "active_model plugin: currently requires forking, and doesn't work on windows or Java"
end
