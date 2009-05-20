require File.join(File.dirname(__FILE__), "spec_helper")

describe "Sequel::Plugins::LazyAttributes" do
  before do
    class ::LazyAttributesModel < Sequel::Model(:la)
      plugin :lazy_attributes
      columns :id, :name
      meta_def(:columns){[:id, :name]}
      lazy_attributes :name
      meta_def(:columns){:id}
      ds = dataset
      def ds.fetch_rows(sql)
        execute(sql)
        select = @opts[:select]
        where = @opts[:where]
        if !where
          if select.include?(:name)
            yield(:id=>1, :name=>'1')
            yield(:id=>2, :name=>'2')
          else
            yield(:id=>1)
            yield(:id=>2)
          end
        else
          i = where.args.last
          i = i.instance_variable_get(:@array) if i.is_a?(Sequel::SQL::SQLArray)
          Array(i).each do |x|
            if sql =~ /SELECT name FROM/
              yield(:name=>x.to_s)
            else
              yield(:id=>x, :name=>x.to_s)
            end
          end
        end
      end
    end
    @c = ::LazyAttributesModel
    @ds = LazyAttributesModel.dataset
    MODEL_DB.reset
  end
  after do
    Object.send(:remove_const, :LazyAttributesModel)
  end

  it "should remove the attributes given from the SELECT columns of the model's dataset" do
    @ds.opts[:select].should == [:id]
    @ds.sql.should == 'SELECT id FROM la'
  end

  it "should lazily load the attribute for a single model object if there is an active identity map" do
    @c.with_identity_map do
      m = @c.first
      m.values.should == {:id=>1}
      m.name.should == '1'
      m.values.should == {:id=>1, :name=>'1'}
      MODEL_DB.sqls.should == ['SELECT id FROM la LIMIT 1', 'SELECT name FROM la WHERE (id = 1) LIMIT 1']
    end
  end

  it "should lazily load the attribute for a single model object if there is no active identity map" do
    m = @c.first
    m.values.should == {:id=>1}
    m.name.should == '1'
    m.values.should == {:id=>1, :name=>'1'}
    MODEL_DB.sqls.should == ['SELECT id FROM la LIMIT 1', 'SELECT name FROM la WHERE (id = 1) LIMIT 1']
  end

  it "should not lazily load the attribute for a single model object if the value already exists" do
    @c.with_identity_map do
      m = @c.first
      m.values.should == {:id=>1}
      m[:name] = '1'
      m.name.should == '1'
      m.values.should == {:id=>1, :name=>'1'}
      MODEL_DB.sqls.should == ['SELECT id FROM la LIMIT 1']
    end
  end

  it "should not lazily load the attribute for a single model object if it is a new record" do
    @c.with_identity_map do
      m = @c.new
      m.values.should == {}
      m.name.should == nil
      MODEL_DB.sqls.should == []
    end
  end

  it "should eagerly load the attribute for all model objects reteived with it" do
    @c.with_identity_map do
      ms = @c.all
      ms.map{|m| m.values}.should == [{:id=>1}, {:id=>2}]
      ms.map{|m| m.name}.should == %w'1 2'
      ms.map{|m| m.values}.should == [{:id=>1, :name=>'1'}, {:id=>2, :name=>'2'}]
      MODEL_DB.sqls.should == ['SELECT id FROM la', 'SELECT id, name FROM la WHERE (id IN (1, 2))']
    end
  end
end
