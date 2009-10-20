require File.join(File.dirname(__FILE__), "spec_helper")

describe "Sequel::Plugins::ValidationHelpers" do
  before do
    @c = Class.new(Sequel::Model) do
      def self.set_validations(&block)
        define_method(:validate, &block)
      end
      columns :value
    end
    @c.plugin :validation_helpers
    @m = @c.new
  end

  specify "should take an :allow_blank option" do
    @c.set_validations{validates_format(/.+_.+/, :value, :allow_blank=>true)}
    @m.value = 'abc_'
    @m.should_not be_valid
    @m.value = '1_1'
    @m.should be_valid
    o = Object.new
    @m.value = o
    @m.should_not be_valid
    def o.blank?
      true
    end
    @m.should be_valid
  end

  specify "should take an :allow_missing option" do
    @c.set_validations{validates_format(/.+_.+/, :value, :allow_missing=>true)}
    @m.values.clear
    @m.should be_valid
    @m.value = nil
    @m.should_not be_valid
    @m.value = '1_1'
    @m.should be_valid
  end

  specify "should take an :allow_nil option" do
    @c.set_validations{validates_format(/.+_.+/, :value, :allow_nil=>true)}
    @m.value = 'abc_'
    @m.should_not be_valid
    @m.value = '1_1'
    @m.should be_valid
    @m.value = nil
    @m.should be_valid
  end

  specify "should take a :message option" do
    @c.set_validations{validates_format(/.+_.+/, :value, :message=>"is so blah")}
    @m.value = 'abc_'
    @m.should_not be_valid
    @m.errors.full_messages.should == ['value is so blah']
    @m.value = '1_1'
    @m.should be_valid
  end
  
  specify "should allow a proc for the :message option" do
    @c.set_validations{validates_format(/.+_.+/, :value, :message=>proc{|f| "doesn't match #{f.inspect}"})}
    @m.value = 'abc_'
    @m.should_not be_valid
    @m.errors.should == {:value=>["doesn't match /.+_.+/"]}
  end

  specify "should take multiple attributes in the same call" do
    @c.columns :value, :value2
    @c.set_validations{validates_presence([:value, :value2])}
    @m.should_not be_valid
    @m.value = 1
    @m.should_not be_valid
    @m.value2 = 1
    @m.should be_valid
  end
  
  specify "should support modifying default validation error messages for all models" do
    @c.set_validations{validates_presence(:value)}
    @m.should_not be_valid
    @m.errors.should == {:value=>['is not present']}
    p = Sequel::Plugins::ValidationHelpers::MESSAGE_PROCS[:presence]
    Sequel::Plugins::ValidationHelpers::MESSAGE_PROCS[:presence] = lambda{"was not entered"}
    @m.should_not be_valid
    @m.errors.should == {:value=>["was not entered"]}
    @m.value = 1
    @m.should be_valid
    
    c = Class.new(Sequel::Model)
    c.class_eval do
      plugin :validation_helpers
      attr_accessor :value
      def validate
        validates_presence(:value)
      end
    end
    m = c.new
    m.should_not be_valid
    m.errors.should == {:value=>["was not entered"]}
    Sequel::Plugins::ValidationHelpers::MESSAGE_PROCS[:presence] = p
  end
  
  specify "should support modifying default validation error messages for a particular model" do
    @c.set_validations{validates_presence(:value)}
    @m.should_not be_valid
    @m.errors.should == {:value=>['is not present']}
    @c.class_eval do
      def default_validation_error_message_proc(type)
        proc{'was not entered'}
      end
    end
    @m.should_not be_valid
    @m.errors.should == {:value=>["was not entered"]}
    @m.value = 1
    @m.should be_valid
    
    c = Class.new(Sequel::Model)
    c.class_eval do
      plugin :validation_helpers
      attr_accessor :value
      def validate
        validates_presence(:value)
      end
    end
    m = c.new
    m.should_not be_valid
    m.errors.should == {:value=>['is not present']}
  end

  specify "should support validates_exact_length" do
    @c.set_validations{validates_exact_length(3, :value)}
    @m.should_not be_valid
    @m.value = '123'
    @m.should be_valid
    @m.value = '12'
    @m.should_not be_valid
    @m.value = '1234'
    @m.should_not be_valid
  end
  
  specify "should support validate_format" do
    @c.set_validations{validates_format(/.+_.+/, :value)}
    @m.value = 'abc_'
    @m.should_not be_valid
    @m.value = 'abc_def'
    @m.should be_valid
  end
  
  specify "should support validates_includes with an array" do
    @c.set_validations{validates_includes([1,2], :value)}
    @m.should_not be_valid
    @m.value = 1
    @m.should be_valid
    @m.value = 1.5
    @m.should_not be_valid
    @m.value = 2
    @m.should be_valid    
    @m.value = 3
    @m.should_not be_valid 
  end
  
  specify "should support validates_includes with a range" do
    @c.set_validations{validates_includes(1..4, :value)}
    @m.should_not be_valid
    @m.value = 1
    @m.should be_valid
    @m.value = 1.5
    @m.should be_valid
    @m.value = 0
    @m.should_not be_valid
    @m.value = 5
    @m.should_not be_valid    
  end
  
  specify "should supports validates_integer" do
    @c.set_validations{validates_integer(:value)}
    @m.value = 'blah'
    @m.should_not be_valid
    @m.value = '123'
    @m.should be_valid
    @m.value = '123.1231'
    @m.should_not be_valid
  end
  
  specify "should support validates_length_range" do
    @c.set_validations{validates_length_range(2..5, :value)}
    @m.should_not be_valid
    @m.value = '12345'
    @m.should be_valid
    @m.value = '1'
    @m.should_not be_valid
    @m.value = '123456'
    @m.should_not be_valid
  end

  specify "should support validates_max_length" do
    @c.set_validations{validates_max_length(5, :value)}
    @m.should_not be_valid
    @m.value = '12345'
    @m.should be_valid
    @m.value = '123456'
    @m.should_not be_valid
  end

  specify "should support validates_min_length" do
    @c.set_validations{validates_min_length(5, :value)}
    @m.should_not be_valid
    @m.value = '12345'
    @m.should be_valid
    @m.value = '1234'
    @m.should_not be_valid
  end

  specify "should support validates_not_string" do
    @c.set_validations{validates_not_string(:value)}
    @m.value = 123
    @m.should be_valid
    @m.value = '123'
    @m.should_not be_valid
    @m.errors.full_messages.should == ['value is a string']
    @m.meta_def(:db_schema){{:value=>{:type=>:integer}}}
    @m.should_not be_valid
    @m.errors.full_messages.should == ['value is not a valid integer']
  end

  specify "should support validates_numeric" do
    @c.set_validations{validates_numeric(:value)}
    @m.value = 'blah'
    @m.should_not be_valid
    @m.value = '123'
    @m.should be_valid
    @m.value = '123.1231'
    @m.should be_valid
    @m.value = '+1'
    @m.should be_valid
    @m.value = '-1'
    @m.should be_valid
    @m.value = '+1.123'
    @m.should be_valid
    @m.value = '-0.123'
    @m.should be_valid
    @m.value = '-0.123E10'
    @m.should be_valid
    @m.value = '32.123e10'
    @m.should be_valid
    @m.value = '+32.123E10'
    @m.should be_valid
    @m.should be_valid
    @m.value = '.0123'
  end

  specify "should support validates_presence" do
    @c.set_validations{validates_presence(:value)}
    @m.should_not be_valid
    @m.value = ''
    @m.should_not be_valid
    @m.value = 1234
    @m.should be_valid
    @m.value = nil
    @m.should_not be_valid
    @m.value = true
    @m.should be_valid
    @m.value = false
    @m.should be_valid
    @m.value = Time.now
    @m.should be_valid
  end
  
  it "should support validates_unique with a single attribute" do
    @c.columns(:id, :username, :password)
    @c.set_dataset MODEL_DB[:items]
    @c.set_validations{validates_unique(:username)}
    @c.dataset.extend(Module.new {
      def fetch_rows(sql)
        @db << sql
        
        case sql
        when /COUNT.*username = '0records'/
          yield({:v => 0})
        when /COUNT.*username = '1record'/
          yield({:v => 1})
        end
      end
    })
    
    @user = @c.new(:username => "0records", :password => "anothertest")
    @user.should be_valid
    @user = @c.load(:id=>3, :username => "0records", :password => "anothertest")
    @user.should be_valid

    @user = @c.new(:username => "1record", :password => "anothertest")
    @user.should_not be_valid
    @user.errors.full_messages.should == ['username is already taken']
    @user = @c.load(:id=>4, :username => "1record", :password => "anothertest")
    @user.should_not be_valid
    @user.errors.full_messages.should == ['username is already taken']

    ds1 = @c.dataset.filter([[:username, '0records']])
    ds2 = ds1.exclude(:id=>1)
    @c.dataset.should_receive(:filter).with([[:username, '0records']]).twice.and_return(ds1)
    ds1.should_receive(:exclude).with(:id=>1).once.and_return(ds2)

    @user = @c.load(:id=>1, :username => "0records", :password => "anothertest")
    @user.should be_valid
    MODEL_DB.sqls.last.should == "SELECT COUNT(*) AS count FROM items WHERE ((username = '0records') AND (id != 1)) LIMIT 1"
    @user = @c.new(:username => "0records", :password => "anothertest")
    @user.should be_valid
    MODEL_DB.sqls.last.should == "SELECT COUNT(*) AS count FROM items WHERE (username = '0records') LIMIT 1"
  end
  
  it "should support validates_unique with multiple attributes" do
    @c.columns(:id, :username, :password)
    @c.set_dataset MODEL_DB[:items]
    @c.set_validations{validates_unique([:username, :password])}
    @c.dataset.extend(Module.new {
      def fetch_rows(sql)
        @db << sql
        
        case sql
        when /COUNT.*username = '0records'/
          yield({:v => 0})
        when /COUNT.*username = '1record'/
          yield({:v => 1})
        end
      end
    })
    
    @user = @c.new(:username => "0records", :password => "anothertest")
    @user.should be_valid
    @user = @c.load(:id=>3, :username => "0records", :password => "anothertest")
    @user.should be_valid

    @user = @c.new(:username => "1record", :password => "anothertest")
    @user.should_not be_valid
    @user.errors.full_messages.should == ['username and password is already taken']
    @user = @c.load(:id=>4, :username => "1record", :password => "anothertest")
    @user.should_not be_valid
    @user.errors.full_messages.should == ['username and password is already taken']

    ds1 = @c.dataset.filter([[:username, '0records'], [:password, 'anothertest']])
    ds2 = ds1.exclude(:id=>1)
    @c.dataset.should_receive(:filter).with([[:username, '0records'], [:password, 'anothertest']]).twice.and_return(ds1)
    ds1.should_receive(:exclude).with(:id=>1).once.and_return(ds2)

    @user = @c.load(:id=>1, :username => "0records", :password => "anothertest")
    @user.should be_valid
    MODEL_DB.sqls.last.should == "SELECT COUNT(*) AS count FROM items WHERE (((username = '0records') AND (password = 'anothertest')) AND (id != 1)) LIMIT 1"
    @user = @c.new(:username => "0records", :password => "anothertest")
    @user.should be_valid
    MODEL_DB.sqls.last.should == "SELECT COUNT(*) AS count FROM items WHERE ((username = '0records') AND (password = 'anothertest')) LIMIT 1"
  end

  it "should support validates_unique with a block" do
    @c.columns(:id, :username, :password)
    @c.set_dataset MODEL_DB[:items]
    @c.set_validations{validates_unique(:username){|ds| ds.filter(:active)}}
    @c.dataset.extend(Module.new {
      def fetch_rows (sql)
        @db << sql
        yield({:v => 0})
      end
    })
    
    MODEL_DB.reset
    @c.new(:username => "0records", :password => "anothertest").should be_valid
    @c.load(:id=>3, :username => "0records", :password => "anothertest").should be_valid
    MODEL_DB.sqls.should == ["SELECT COUNT(*) AS count FROM items WHERE ((username = '0records') AND active) LIMIT 1",
                    "SELECT COUNT(*) AS count FROM items WHERE (((username = '0records') AND active) AND (id != 3)) LIMIT 1"]
  end
end 
