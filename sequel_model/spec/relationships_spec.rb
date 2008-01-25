require File.join(File.dirname(__FILE__), "spec_helper")

#__END__

# class Post < Sequel::Model
#   relationships do
#     has :one,  :blog, :required => true, :normalized => false # uses a blog_id field, which cannot be null, in the Post model
#     has :one,  :account # uses a join table called accounts_posts to link the post with it's account.
#     has :many, :comments # uses a comments_posts join table
#     has :many, :authors, :required => true  # authors_posts join table, requires at least one author
#   end
# end
describe Sequel::Model, "relationships" do
  before :all do
    class Smurf < Sequel::Model
    end
  end
  
  after :each do
    Smurf.model_relationships.clear
  end
  
  describe "has" do
    
    it "should raise an exception if an arity {:one, :many} is not specified" do
      Smurf.should_not_receive(:auto_create_join_table).with(:smurfette, {}).and_return(true)
      Smurf.should_not_receive(:relationship_exists?).with(:one, :smurfette).and_return(true)
      Smurf.stub!(:after_initialize)
      lambda {
      class Smurf
        relationships do
          has :sex, :with_smurfette
        end
      end
      }.should raise_error Sequel::Error, "Arity must be specified {:one, :many}." 
    end
    
    it "should check to see if the relationship exists" do
      Smurf.should_not_receive(:relationship_exists?).with(:one, :smurfette).and_return(true)
      Smurf.stub!(:after_initialize)
      lambda {
      class Smurf
        relationships do
          has :sex, :with_smurfette
        end
      end
      }.should raise_error Sequel::Error, "Arity must be specified {:one, :many}."
    end
    
    it "should raise an exception if the relationship has already been specified" do
      Smurf.should_receive(:relationship_exists?).with(:one, :smurfette).and_return(true)
      Smurf.stub!(:after_initialize)
      lambda {
      class Smurf
        relationships do
          has :one, :smurfette
        end
      end
      }.should raise_error Sequel::Error, "The relationship 'Smurf has one smurfette' is already defined."
    end
    
    it "should establish a has :one relationship" do
      Smurf.stub!(:auto_create_join_table)
      Smurf.should_receive(:relationship_exists?).with(:one, :smurfette).and_return(false)
      Smurf.should_receive(:after_initialize)
      class Smurf
        relationships do
          has :one, :smurfette 
        end
      end
      
      @smurf = Smurf.new
    
    end
    
    it "should establish a has :many relationship" do
      Smurf.should_receive(:auto_create_join_table).with(:smurfette, {}).and_return(true)
      Smurf.should_receive(:relationship_exists?).with(:many, :smurfette).and_return(false)
      Smurf.should_receive(:after_initialize)
      class Smurf
        relationships do
          has :many, :smurfette 
        end
      end
      
      @smurf = Smurf.new
    end
    
    it "should call the auto_create_join_table method" do
      Smurf.should_receive(:auto_create_join_table).with(:smurfette, {}).and_return(true)

      class Smurf
        relationships do
          has :one, :smurfette
        end
      end
    end
    
    it "should store the relationship to ensure there is no duplication" do
      class Smurf
        relationships do
          has :one, :smurfette
          has :many, :legs
        end
      end
      
      Smurf.model_relationships.should == [
        {:arity=>:one, :options=>{}, :klass=>:smurfette},
        {:arity=>:many, :options=>{}, :klass=>:legs}
      ]
      
    end
    
    it "should create an instance method for the has one relationship" do
      class Smurf
        relationships do
          has :one, :smurfette
        end
      end
      
      @smurf = Smurf.new
      @smurf.should respond_to(:smurfette)
      @smurf.should_not respond_to(:smurfettes)
    end
    
    it "should create an instance method for the has many relationship" do
      class Smurf
        relationships do
          has :one, :smurfettes
        end
      end
      
      @smurf = Smurf.new
      @smurf.should respond_to(:smurfettes)
      @smurf.should_not respond_to(:smurf)
    end
  end
  
  describe "has_one" do
    it "should be an alias for has :one" do
      Smurf.should_receive(:has).with(:one, :smurfette, {})
      class Smurf
        relationships do
          has_one :smurfette
        end
      end
    end
  end
  
  describe "has_many" do
    it "should be an alias for has :many" do
      Smurf.should_receive(:has).with(:many, :smurfette, {})
      class Smurf
        relationships do
          has_many :smurfette
        end
      end
    end
  end

  describe "belongs_to" do
    it "should be an alias for has :one" do
      Smurf.should_receive(:has).with(:one, :smurfette, {})
      class Smurf
        relationships do
          belongs_to :smurfette
        end
      end
    end
  end
  
  describe "has_relationships?" do
    it "should return true if the class has any relationships" do
      class Smurf
        relationships do
          belongs_to :smurfette
        end
      end
      
      Smurf.has_relationships?.should be_true
    end
    
    it "should return false if the class has no relationships" do
      class Smurf
      end
      
      Smurf.has_relationships?.should be_false
    end
  end

end
