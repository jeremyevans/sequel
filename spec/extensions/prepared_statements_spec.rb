require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "prepared_statements plugin" do
  before do
    @db = Sequel.mock(:fetch=>{:id=>1, :name=>'foo', :i=>2}, :autoid=>proc{|sql| 1}, :numrows=>1, :servers=>{:read_only=>{}})
    @c = Class.new(Sequel::Model(@db[:people]))
    @c.columns :id, :name, :i
    @columns = "id, name, i"
    @c.plugin :prepared_statements
    @p = @c.load(:id=>1, :name=>'foo', :i=>2)
    @ds = @c.dataset
    @db.sqls
  end

  specify "should correctly lookup by primary key" do
    @c[1].should == @p
    @db.sqls.should == ["SELECT id, name, i FROM people WHERE (id = 1) LIMIT 1 -- read_only"]
  end 

  shared_examples_for "prepared_statements plugin" do
    specify "should correctly delete instance" do
      @p.destroy.should == @p
      @db.sqls.should == ["DELETE FROM people WHERE (id = 1)"]
    end

    specify "should correctly update instance" do
      @p.update(:name=>'bar').should == @c.load(:id=>1, :name=>'bar', :i => 2)
      @db.sqls.should == ["UPDATE people SET name = 'bar' WHERE (id = 1)"]
    end

    specify "should correctly create instance" do
      @c.create(:name=>'foo').should == @c.load(:id=>1, :name=>'foo', :i => 2)
      @db.sqls.should == ["INSERT INTO people (name) VALUES ('foo')", "SELECT #{@columns} FROM people WHERE (id = 1) LIMIT 1"]
    end

    specify "should correctly create instance if dataset supports insert_select" do
      @c.dataset_module do
        def supports_insert_select?
          true
        end
        def supports_returning?(type)
          true
        end
        def insert_select(h)
          self._fetch = {:id=>1, :name=>'foo', :i => 2}
          server(:default).with_sql_first(insert_select_sql(h))
        end
        def insert_select_sql(*v)
          "#{insert_sql(*v)} RETURNING #{(opts[:returning] && !opts[:returning].empty?) ? opts[:returning].map{|c| literal(c)}.join(', ') : '*'}"
        end
      end
      @c.create(:name=>'foo').should == @c.load(:id=>1, :name=>'foo', :i => 2)
      @db.sqls.should == ["INSERT INTO people (name) VALUES ('foo') RETURNING #{@columns}"]
    end
  end

  it_should_behave_like "prepared_statements plugin"

  describe "when #use_prepared_statements_for? returns false" do
    before do
      @columns = "*"
      @c.class_eval{def use_prepared_statements_for?(type) false end}
    end

    it_should_behave_like "prepared_statements plugin"
  end

  specify "should work correctly when subclassing" do
    c = Class.new(@c)
    c[1].should == c.load(:id=>1, :name=>'foo', :i=>2)
    @db.sqls.should == ["SELECT id, name, i FROM people WHERE (id = 1) LIMIT 1 -- read_only"]
  end 

  describe " with placeholder type specifiers" do 
    before do
      @ds.meta_def(:requires_placeholder_type_specifiers?){true}
    end

    specify "should correctly handle without schema type" do
      @c[1].should == @p
      @db.sqls.should == ["SELECT id, name, i FROM people WHERE (id = 1) LIMIT 1 -- read_only"]
    end

    specify "should correctly handle with schema type" do
      @c.db_schema[:id][:type] = :integer
      ds = @c.send(:prepared_lookup)
      def ds.literal_symbol_append(sql, v)
        if @opts[:bind_vars] and match = /\A\$(.*)\z/.match(v.to_s)
          s = match[1].split('__')[0].to_sym
          if prepared_arg?(s)
            literal_append(sql, prepared_arg(s))
          else
            sql << v.to_s
          end
        else
          super
        end
      end
      @c[1].should == @p
      @db.sqls.should == ["SELECT id, name, i FROM people WHERE (id = 1) LIMIT 1 -- read_only"]
    end 
  end
end
