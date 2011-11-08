require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::AssociationPks" do
  before do
    @db = Sequel.mock(:fetch=>proc do |sql|
      case sql
      when "SELECT id FROM albums WHERE (albums.artist_id = 1)"
        [{:id=>1}, {:id=>2}, {:id=>3}]
      when /SELECT tag_id FROM albums_tags WHERE \(album_id = (\d)\)/
        a = []
        a << {:tag_id=>1} if $1 == '1'
        a << {:tag_id=>2} if $1 != '3'
        a << {:tag_id=>3} if $1 == '2'
        a
      end
    end)
    @Artist = Class.new(Sequel::Model(@db[:artists]))
    @Artist.columns :id
    @Album= Class.new(Sequel::Model(@db[:albums]))
    @Album.columns :id, :artist_id
    @Tag = Class.new(Sequel::Model(@db[:tags]))
    @Tag.columns :id
    @Artist.plugin :association_pks
    @Album.plugin :association_pks
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id
    @Album.many_to_many :tags, :class=>@Tag, :join_table=>:albums_tags, :left_key=>:album_id
    @db.sqls
  end

  specify "should return correct associated pks for one_to_many associations" do
    @Artist.load(:id=>1).album_pks.should == [1,2,3]
    @Artist.load(:id=>2).album_pks.should == []
  end

  specify "should return correct associated pks for many_to_many associations" do
    @Album.load(:id=>1).tag_pks.should == [1, 2]
    @Album.load(:id=>2).tag_pks.should == [2, 3]
    @Album.load(:id=>3).tag_pks.should == []
  end

  specify "should set associated pks correctly for a one_to_many association" do
    @Artist.load(:id=>1).album_pks = [1, 2]
    @db.sqls.should == ["UPDATE albums SET artist_id = 1 WHERE (id IN (1, 2))",
      "UPDATE albums SET artist_id = NULL WHERE ((albums.artist_id = 1) AND (id NOT IN (1, 2)))"]
  end

  specify "should use associated class's primary key for a one_to_many association" do
    @Album.set_primary_key :foo
    @Artist.load(:id=>1).album_pks = [1, 2]
    @db.sqls.should == ["UPDATE albums SET artist_id = 1 WHERE (foo IN (1, 2))",
      "UPDATE albums SET artist_id = NULL WHERE ((albums.artist_id = 1) AND (foo NOT IN (1, 2)))"]
  end

  specify "should set associated pks correctly for a many_to_many association" do
    @Album.load(:id=>2).tag_pks = [1, 3]
    sqls = @db.sqls
    sqls[0].should == "DELETE FROM albums_tags WHERE ((album_id = 2) AND (tag_id NOT IN (1, 3)))"
    sqls[1].should == 'SELECT tag_id FROM albums_tags WHERE (album_id = 2)'
    sqls[2].should =~ /INSERT INTO albums_tags \((album_id, tag_id|tag_id, album_id)\) VALUES \((2, 1|1, 2)\)/
    sqls.length.should == 3
  end

  specify "should use transactions if the object is configured to use transactions" do
    artist = @Artist.load(:id=>1)
    artist.use_transactions = true
    artist.album_pks = [1, 2]
    @db.sqls.should == ["BEGIN",
      "UPDATE albums SET artist_id = 1 WHERE (id IN (1, 2))",
      "UPDATE albums SET artist_id = NULL WHERE ((albums.artist_id = 1) AND (id NOT IN (1, 2)))",
      "COMMIT"]

    album = @Album.load(:id=>2)
    album.use_transactions = true
    album.tag_pks = [1, 3]
    sqls = @db.sqls
    sqls[0].should == "BEGIN"
    sqls[1].should == "DELETE FROM albums_tags WHERE ((album_id = 2) AND (tag_id NOT IN (1, 3)))"
    sqls[2].should == 'SELECT tag_id FROM albums_tags WHERE (album_id = 2)'
    sqls[3].should =~ /INSERT INTO albums_tags \((album_id, tag_id|tag_id, album_id)\) VALUES \((2, 1|1, 2)\)/
    sqls[4].should == "COMMIT"
    sqls.length.should == 5
  end

  specify "should automatically convert keys to numbers if the primary key is an integer for one_to_many associations" do
    @Album.db_schema[:id][:type] = :integer
    @Artist.load(:id=>1).album_pks = %w'1 2'
    @db.sqls.should == ["UPDATE albums SET artist_id = 1 WHERE (id IN (1, 2))",
      "UPDATE albums SET artist_id = NULL WHERE ((albums.artist_id = 1) AND (id NOT IN (1, 2)))"]
  end

  specify "should not automatically convert keys if the primary key is not an integer for many_to_many associations" do
    @Album.db_schema[:id][:type] = :string
    @Artist.load(:id=>1).album_pks = %w'1 2'
    @db.sqls.should == ["UPDATE albums SET artist_id = 1 WHERE (id IN ('1', '2'))",
      "UPDATE albums SET artist_id = NULL WHERE ((albums.artist_id = 1) AND (id NOT IN ('1', '2')))"]
  end

  specify "should automatically convert keys to numbers if the primary key is an integer for one_to_many associations" do
    @Tag.db_schema[:id][:type] = :integer
    @Album.load(:id=>2).tag_pks = %w'1 3'
    sqls = @db.sqls
    sqls[0].should == "DELETE FROM albums_tags WHERE ((album_id = 2) AND (tag_id NOT IN (1, 3)))"
    sqls[1].should == 'SELECT tag_id FROM albums_tags WHERE (album_id = 2)'
    sqls[2].should =~ /INSERT INTO albums_tags \((album_id, tag_id|tag_id, album_id)\) VALUES \((2, 1|1, 2)\)/
    sqls.length.should == 3
  end

  specify "should not automatically convert keys to numbers if the primary key is an integer for many_to_many associations" do
    @Tag.db_schema[:id][:type] = :string
    @Album.load(:id=>2).tag_pks = %w'1 3'
    sqls = @db.sqls
    sqls[0].should == "DELETE FROM albums_tags WHERE ((album_id = 2) AND (tag_id NOT IN ('1', '3')))"
    sqls[1].should == 'SELECT tag_id FROM albums_tags WHERE (album_id = 2)'
    sqls[2].should =~ /INSERT INTO albums_tags \((album_id, tag_id|tag_id, album_id)\) VALUES \((2, '1'|'1', 2)\)/
    sqls[3].should =~ /INSERT INTO albums_tags \((album_id, tag_id|tag_id, album_id)\) VALUES \((2, '3'|'3', 2)\)/
    sqls.length.should == 4
  end

end
