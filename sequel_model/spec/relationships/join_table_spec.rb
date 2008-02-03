__END__


describe Sequel::Model::JoinTable do  

  describe "class methods" do

    before(:all) do
      class Person < Sequel::Model
        set_primary_key [:first_name, :last_name, :middle_name]
      end
      class Address < Sequel::Model
        set_primary_key [:street,:suite,:zip]
      end
      class Monkey < Sequel::Model
        # primary key should be :id
      end
    end

    describe "keys" do

      it "should return an array of the primary keys for a complex primary key" do      
        # @join_table = Sequel::Model::JoinTable.new :person, :address
        Sequel::Model::JoinTable.keys(Person).should eql(["person_first_name", "person_last_name", "person_middle_name"])
        Sequel::Model::JoinTable.keys(Address).should eql(["address_street", "address_suite", "address_zip"])
        Sequel::Model::JoinTable.keys(Monkey).should eql(["monkey_id"])
      end

    end

  end

  describe "instance methods" do

    before(:each) do
      class Post < Sequel::Model(:posts); end
      class Comment < Sequel::Model(:comments); end
      class Article < Sequel::Model(:articles); end
      @join_table = Sequel::Model::JoinTable.new :post, :comment
      @join_table_plural = Sequel::Model::JoinTable.new :posts, :comments
      @join_table_string = Sequel::Model::JoinTable.new "posts", "comments"
      @db = mock("db instance")
    end

    describe "name" do

      it "should have a proper join table name" do
        @join_table.name.should == "comments_posts"
        @join_table_plural.name.should == "comments_posts"
        @join_table_string.name.should == "comments_posts"
      end

    end

    describe "join class" do

      it "should define the join class if it does not exist" do
        class Foo < Sequel::Model(:foos); end
        class Bar < Sequel::Model(:bars); end
        Sequel::Model::JoinTable.new :foos, :bars
        defined?(FooBar).should_not be_nil
      end

      it "should not redefine the join class if it already exists" do
        undef ArticleComment if defined?(ArticleComment)
        class ArticleComment < Sequel::Model
          set_primary_key :id
        end
        @join_table = Sequel::Model::JoinTable.new :article, :comment
        ArticleComment.primary_key.should == :id
      end

      it "should return the join class" do
        @join_table.join_class.should eql(PostComment)
      end

    end

    describe "exists?" do

      before :each do
        @join_table.should_receive(:db).and_return(@db)
        @db.should_receive(:[]).with("comments_posts").and_return(@db)
      end

      it "should indicate if the table exists" do
        @db.should_receive(:table_exists?).and_return(true)
        @join_table.exists?.should == true
      end

      it "should indicate if the table does not exist" do
        @db.should_receive(:table_exists?).and_return(false)
        @join_table.exists?.should == false
      end

    end

    describe "create" do

      it "should create the table if it doesn't exist" do
        @join_table.should_receive(:exists?).and_return(false)
        @join_table.should_receive(:db).and_return(@db)
        @db.should_receive(:create_table).with(:comments_posts)
        @join_table.create.should be_true
      end

      it "should fail to create the table if it does exist" do
        @join_table.should_receive(:exists?).and_return(true)
        @join_table.create.should be_false
      end

    end

    describe "create!" do

      it "should force the creation of the table it exists" do
        @join_table.should_receive(:exists?).and_return(true)
        @join_table.should_receive(:db).and_return(@db)
        @db.should_receive(:drop_table).with("comments_posts")
        @join_table.should_receive(:create).and_return(true)
        @join_table.create!.should be_true
      end

    end

    describe "db" do

      it "should have access to the db object" do
        class Post; end

        Post.should_receive(:db).and_return(@db)
        @join_table.db.should == @db
      end

    end

  end

end
