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
      when "SELECT first_name, last_name FROM vocalists WHERE (vocalists.album_id = 1)"
        [{:first_name=>"F1", :last_name=>"L1"}, {:first_name=>"F2", :last_name=>"L2"}]
      when /SELECT first_name, last_name FROM albums_vocalists WHERE \(album_id = (\d)\)/
        a = []
        a << {:first_name=>"F1", :last_name=>"L1"} if $1 == '1'
        a << {:first_name=>"F2", :last_name=>"L2"} if $1 != '3'
        a << {:first_name=>"F3", :last_name=>"L3"} if $1 == '2'
        a
      end
    end)
    @Artist = Class.new(Sequel::Model(@db[:artists]))
    @Artist.columns :id
    @Album = Class.new(Sequel::Model(@db[:albums]))
    @Album.columns :id, :artist_id
    @Tag = Class.new(Sequel::Model(@db[:tags]))
    @Tag.columns :id
    @Vocalist = Class.new(Sequel::Model(@db[:vocalists]))
    @Vocalist.columns :first_name, :last_name, :album_id
    @Vocalist.set_primary_key [:first_name, :last_name]
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

  specify "should return correct associated cpks for one_to_many associations" do
    @Album.one_to_many :vocalists, :class=>@Vocalist, :key=>:album_id
    @Album.load(:id=>1).vocalist_pks.should == [["F1", "L1"], ["F2", "L2"]]
    @Album.load(:id=>2).vocalist_pks.should == []
  end

  specify "should return correct associated cpks for many_to_many associations" do
    @Album.many_to_many :vocalists, :class=>@Vocalist, :join_table=>:albums_vocalists, :left_key=>:album_id, :right_key=>[:first_name, :last_name]
    @Album.load(:id=>1).vocalist_pks.should == [["F1", "L1"], ["F2", "L2"]]
    @Album.load(:id=>2).vocalist_pks.should == [["F2", "L2"], ["F3", "L3"]]
    @Album.load(:id=>3).vocalist_pks.should == []
  end

  specify "should set associated cpks correctly for a one_to_many association" do
    @Album.one_to_many :vocalists, :class=>@Vocalist, :key=>:album_id
    @Album.load(:id=>1).vocalist_pks = [["F1", "L1"], ["F2", "L2"]]
    @db.sqls.should == ["UPDATE vocalists SET album_id = 1 WHERE ((first_name, last_name) IN (('F1', 'L1'), ('F2', 'L2')))",
      "UPDATE vocalists SET album_id = NULL WHERE ((vocalists.album_id = 1) AND ((first_name, last_name) NOT IN (('F1', 'L1'), ('F2', 'L2'))))"]
  end

  specify "should set associated cpks correctly for a many_to_many association" do
    @Album.many_to_many :vocalists, :class=>@Vocalist, :join_table=>:albums_vocalists, :left_key=>:album_id, :right_key=>[:first_name, :last_name]
    @Album.load(:id=>2).vocalist_pks = [["F1", "L1"], ["F2", "L2"]]
    sqls = @db.sqls
    sqls[0].should == "DELETE FROM albums_vocalists WHERE ((album_id = 2) AND ((first_name, last_name) NOT IN (('F1', 'L1'), ('F2', 'L2'))))"
    sqls[1].should == 'SELECT first_name, last_name FROM albums_vocalists WHERE (album_id = 2)'
    sqls[2] =~ /INSERT INTO albums_vocalists \((.*)\) VALUES \((.*)\)/
    Hash[$1.split(', ').zip($2.split(', '))].should == {"first_name"=>"'F1'", "last_name"=>"'L1'", "album_id"=>"2"}
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
