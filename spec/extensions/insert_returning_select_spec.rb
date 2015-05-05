require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::InsertReturningSelect" do
  before do
    @db = Sequel.mock(:fetch=>{:id=>1, :x=>2}, :autoid=>1)
    @db.extend_datasets do
      def supports_returning?(_) true end
      def insert_select(*v) with_sql_first("#{insert_sql(*v)} RETURNING #{opts[:returning].map{|x| literal(x)}.join(', ')}") end
    end
    @Album = Class.new(Sequel::Model(@db[:albums].select(:id, :x)))
    @Album.columns :id, :x
    @db.sqls
  end

  it "should add a returning clause when inserting using selected columns" do
    @Album.plugin :insert_returning_select
    @Album.create(:x=>2).must_equal @Album.load(:id=>1, :x=>2)
    @db.sqls.must_equal ['INSERT INTO albums (x) VALUES (2) RETURNING id, x']
  end

  it "should not add a returning clause if selection does not consist of just columns" do
    @Album.dataset = @Album.dataset.select_append(Sequel.as(1, :b))
    @Album.plugin :insert_returning_select
    @db.sqls.clear
    @Album.create(:x=>2).must_equal @Album.load(:id=>1, :x=>2)
    @db.sqls.must_equal ['INSERT INTO albums (x) VALUES (2)', 'SELECT id, x, 1 AS b FROM albums WHERE (id = 1) LIMIT 1']
  end

  it "should not add a returning clause if database doesn't support it" do
    @db.extend_datasets{def supports_returning?(_) false end}
    @Album.plugin :insert_returning_select
    @Album.create(:x=>2).must_equal @Album.load(:id=>1, :x=>2)
    @db.sqls.must_equal ['INSERT INTO albums (x) VALUES (2)', 'SELECT id, x FROM albums WHERE (id = 1) LIMIT 1']
  end

  it "should work correctly with subclasses" do
    c = Class.new(Sequel::Model)
    c.plugin :insert_returning_select
    b = Class.new(c)
    b.columns :id, :x
    b.dataset = @db[:albums].select(:id, :x)
    @db.sqls.clear
    b.create(:x=>2).must_equal b.load(:id=>1, :x=>2)
    @db.sqls.must_equal ['INSERT INTO albums (x) VALUES (2) RETURNING id, x']
  end
end
