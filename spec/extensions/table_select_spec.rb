require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::TableSelect" do
  before do
    @Album = Class.new(Sequel::Model(Sequel.mock[:albums]))
  end

  it "should add a table.* selection to existing dataset without explicit selection" do
    @Album.plugin :table_select
    @Album.dataset.sql.should == 'SELECT albums.* FROM albums'

    @Album.dataset = :albs
    @Album.dataset.sql.should == 'SELECT albs.* FROM albs'

    @Album.dataset = Sequel.identifier(:albs)
    @Album.dataset.sql.should == 'SELECT albs.* FROM albs'
  end

  it "should handle qualified tables" do
    @Album.dataset = :s__albums
    @Album.plugin :table_select
    @Album.dataset.sql.should == 'SELECT s.albums.* FROM s.albums'

    @Album.dataset = Sequel.qualify(:s2, :albums)
    @Album.dataset.sql.should == 'SELECT s2.albums.* FROM s2.albums'
  end

  it "should handle aliases" do
    @Album.dataset = :albums___a
    @Album.plugin :table_select
    @Album.dataset.sql.should == 'SELECT a.* FROM albums AS a'

    @Album.dataset = Sequel.as(:albums, :b)
    @Album.dataset.sql.should == 'SELECT b.* FROM albums AS b'

    @Album.dataset = :s__albums___a
    @Album.dataset.sql.should == 'SELECT a.* FROM s.albums AS a'

    @Album.dataset = @Album.db[:albums].from_self
    @Album.dataset.sql.should == 'SELECT t1.* FROM (SELECT * FROM albums) AS t1'

    @Album.dataset = Sequel.as(@Album.db[:albums], :b)
    @Album.dataset.sql.should == 'SELECT b.* FROM (SELECT * FROM albums) AS b'
  end

  it "should not add a table.* selection on existing dataset with explicit selection" do
    @Album.dataset = @Album.dataset.select(:name)
    @Album.plugin :table_select
    @Album.dataset.sql.should == 'SELECT name FROM albums'

    @Album.dataset = @Album.dataset.select(:name, :artist)
    @Album.dataset.sql.should == 'SELECT name, artist FROM albums'
  end

  it "should not add a table.* selection on existing dataset with multiple tables" do
    @Album.dataset = @Album.db.from(:a1, :a2)
    @Album.plugin :table_select
    @Album.dataset.sql.should == 'SELECT * FROM a1, a2'

    @Album.dataset = @Album.db.from(:a1).cross_join(:a2)
    @Album.dataset.sql.should == 'SELECT * FROM a1 CROSS JOIN a2'
  end

  it "works correctly when loaded on model without a dataset" do
    c = Class.new(Sequel::Model)
    c.plugin :table_select
    sc = Class.new(c)
    sc.dataset = :a
    sc.dataset.sql.should == "SELECT a.* FROM a"
  end
end
