require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::AssociationPks" do
  before do
    @db = Sequel.mock(:fetch=>proc do |sql|
      case sql
      when "SELECT id FROM albums WHERE (albums.artist_id = 1)"
        [{:id=>1}, {:id=>2}, {:id=>3}]
      when /SELECT tag_id FROM albums_tags WHERE \(album_id IN \(\((\d)\)\)\)/
        a = []
        a << {:tag_id=>1} if $1 == '1'
        a << {:tag_id=>2} if $1 != '3'
        a << {:tag_id=>3} if $1 == '2'
        a
      when "SELECT first, last FROM vocalists WHERE (vocalists.album_id = 1)"
        [{:first=>"F1", :last=>"L1"}, {:first=>"F2", :last=>"L2"}]
      when /SELECT first, last FROM albums_vocalists WHERE \(album_id IN \(\((\d)\)\)\)/
        a = []
        a << {:first=>"F1", :last=>"L1"} if $1 == '1'
        a << {:first=>"F2", :last=>"L2"} if $1 != '3'
        a << {:first=>"F3", :last=>"L3"} if $1 == '2'
        a
      when "SELECT id FROM instruments WHERE ((instruments.first = 'F1') AND (instruments.last = 'L1'))"
        [{:id=>1}, {:id=>2}]
      when /SELECT instrument_id FROM vocalists_instruments WHERE \(\(first, last\) IN \(\((.*)\)\)\)/
        a = []
        a << {:instrument_id=>1} if $1 == "'F1', 'L1'"
        a << {:instrument_id=>2} if $1 != "'F3', 'L3'"
        a << {:instrument_id=>3} if $1 == "'F2', 'L2'"
        a
      when "SELECT year, week FROM hits WHERE ((hits.first = 'F1') AND (hits.last = 'L1'))"
        [{:year=>1997, :week=>1}, {:year=>1997, :week=>2}]
      when /SELECT year, week FROM vocalists_hits WHERE \(\(first, last\) IN \(\((.*)\)\)\)/
        a = []
        a << {:year=>1997, :week=>1} if $1 == "'F1', 'L1'"
        a << {:year=>1997, :week=>2} if $1 != "'F3', 'L3'"
        a << {:year=>1997, :week=>3} if $1 == "'F2', 'L2'"
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
    @Vocalist.columns :first, :last, :album_id
    @Vocalist.set_primary_key [:first, :last]
    @Instrument = Class.new(Sequel::Model(@db[:instruments]))
    @Instrument.columns :id, :first, :last
    @Hit = Class.new(Sequel::Model(@db[:hits]))
    @Hit.columns :year, :week, :first, :last
    @Hit.set_primary_key [:year, :week]
    @Artist.plugin :association_pks
    @Album.plugin :association_pks
    @Vocalist.plugin :association_pks
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
    sqls[0].should == "DELETE FROM albums_tags WHERE ((album_id IN ((2))) AND (tag_id NOT IN (1, 3)))"
    sqls[1].should == 'SELECT tag_id FROM albums_tags WHERE (album_id IN ((2)))'
    sqls[2].should =~ /INSERT INTO albums_tags \((album_id, tag_id|tag_id, album_id)\) VALUES \((2, 1|1, 2)\)/
    sqls.length.should == 3
  end

  specify "should return correct right-side associated cpks for one_to_many associations" do
    @Album.one_to_many :vocalists, :class=>@Vocalist, :key=>:album_id
    @Album.load(:id=>1).vocalist_pks.should == [["F1", "L1"], ["F2", "L2"]]
    @Album.load(:id=>2).vocalist_pks.should == []
  end

  specify "should return correct right-side associated cpks for many_to_many associations" do
    @Album.many_to_many :vocalists, :class=>@Vocalist, :join_table=>:albums_vocalists, :left_key=>:album_id, :right_key=>[:first, :last]
    @Album.load(:id=>1).vocalist_pks.should == [["F1", "L1"], ["F2", "L2"]]
    @Album.load(:id=>2).vocalist_pks.should == [["F2", "L2"], ["F3", "L3"]]
    @Album.load(:id=>3).vocalist_pks.should == []
  end

  specify "should set associated right-side cpks correctly for a one_to_many association" do
    @Album.one_to_many :vocalists, :class=>@Vocalist, :key=>:album_id
    @Album.load(:id=>1).vocalist_pks = [["F1", "L1"], ["F2", "L2"]]
    @db.sqls.should == ["UPDATE vocalists SET album_id = 1 WHERE ((first, last) IN (('F1', 'L1'), ('F2', 'L2')))",
      "UPDATE vocalists SET album_id = NULL WHERE ((vocalists.album_id = 1) AND ((first, last) NOT IN (('F1', 'L1'), ('F2', 'L2'))))"]
  end

  specify "should set associated right-side cpks correctly for a many_to_many association" do
    @Album.many_to_many :vocalists, :class=>@Vocalist, :join_table=>:albums_vocalists, :left_key=>:album_id, :right_key=>[:first, :last]
    @Album.load(:id=>2).vocalist_pks = [["F1", "L1"], ["F2", "L2"]]
    sqls = @db.sqls
    sqls[0].should == "DELETE FROM albums_vocalists WHERE ((album_id IN ((2))) AND ((first, last) NOT IN (('F1', 'L1'), ('F2', 'L2'))))"
    sqls[1].should == 'SELECT first, last FROM albums_vocalists WHERE (album_id IN ((2)))'
    match = sqls[2].match(/INSERT INTO albums_vocalists \((.*)\) VALUES \((.*)\)/)
    Hash[match[1].split(', ').zip(match[2].split(', '))].should == {"first"=>"'F1'", "last"=>"'L1'", "album_id"=>"2"}
    sqls.length.should == 3
  end

  specify "should return correct associated pks for left-side cpks for one_to_many associations" do
    @Vocalist.one_to_many :instruments, :class=>@Instrument, :key=>[:first, :last]
    @Vocalist.load(:first=>'F1', :last=>'L1').instrument_pks.should == [1, 2]
    @Vocalist.load(:first=>'F2', :last=>'L2').instrument_pks.should == []
  end

  specify "should return correct associated pks for left-side cpks for many_to_many associations" do
    @Vocalist.many_to_many :instruments, :class=>@Instrument, :join_table=>:vocalists_instruments, :left_key=>[:first, :last]
    @Vocalist.load(:first=>'F1', :last=>'L1').instrument_pks.should == [1, 2]
    @Vocalist.load(:first=>'F2', :last=>'L2').instrument_pks.should == [2, 3]
    @Vocalist.load(:first=>'F3', :last=>'L3').instrument_pks.should == []
  end

  specify "should set associated pks correctly for left-side cpks for a one_to_many association" do
    @Vocalist.one_to_many :instruments, :class=>@Instrument, :key=>[:first, :last]
    @Vocalist.load(:first=>'F1', :last=>'L1').instrument_pks = [1, 2]
    sqls = @db.sqls
    sqls[0].should =~ /UPDATE instruments SET (first = 'F1', last = 'L1'|last = 'L1', first = 'F1') WHERE \(id IN \(1, 2\)\)/
    sqls[1].should =~ /UPDATE instruments SET (first = NULL, last = NULL|last = NULL, first = NULL) WHERE \(\(instruments.first = 'F1'\) AND \(instruments.last = 'L1'\) AND \(id NOT IN \(1, 2\)\)\)/
    sqls.length.should == 2
  end

  specify "should set associated pks correctly for left-side cpks for a many_to_many association" do
    @Vocalist.many_to_many :instruments, :class=>@Instrument, :join_table=>:vocalists_instruments, :left_key=>[:first, :last]
    @Vocalist.load(:first=>'F2', :last=>'L2').instrument_pks = [1, 2]
    sqls = @db.sqls
    sqls[0].should == "DELETE FROM vocalists_instruments WHERE (((first, last) IN (('F2', 'L2'))) AND (instrument_id NOT IN (1, 2)))"
    sqls[1].should == "SELECT instrument_id FROM vocalists_instruments WHERE ((first, last) IN (('F2', 'L2')))"
    match = sqls[2].match(/INSERT INTO vocalists_instruments \((.*)\) VALUES \((.*)\)/)
    Hash[match[1].split(', ').zip(match[2].split(', '))].should == {"first"=>"'F2'", "last"=>"'L2'", "instrument_id"=>"1"}
    sqls.length.should == 3
  end

  specify "should return correct right-side associated cpks for left-side cpks for one_to_many associations" do
    @Vocalist.one_to_many :hits, :class=>@Hit, :key=>[:first, :last]
    @Vocalist.load(:first=>'F1', :last=>'L1').hit_pks.should == [[1997, 1], [1997, 2]]
    @Vocalist.load(:first=>'F2', :last=>'L2').hit_pks.should == []
  end

  specify "should return correct right-side associated cpks for left-side cpks for many_to_many associations" do
    @Vocalist.many_to_many :hits, :class=>@Hit, :join_table=>:vocalists_hits, :left_key=>[:first, :last], :right_key=>[:year, :week]
    @Vocalist.load(:first=>'F1', :last=>'L1').hit_pks.should == [[1997, 1], [1997, 2]]
    @Vocalist.load(:first=>'F2', :last=>'L2').hit_pks.should == [[1997, 2], [1997, 3]]
    @Vocalist.load(:first=>'F3', :last=>'L3').hit_pks.should == []
  end

  specify "should set associated right-side cpks correctly for left-side cpks for a one_to_many association" do
    @Vocalist.one_to_many :hits, :class=>@Hit, :key=>[:first, :last], :order=>:week
    @Vocalist.load(:first=>'F1', :last=>'L1').hit_pks = [[1997, 1], [1997, 2]]
    sqls = @db.sqls
    sqls[0].should =~ /UPDATE hits SET (first = 'F1', last = 'L1'|last = 'L1', first = 'F1') WHERE \(\(year, week\) IN \(\(1997, 1\), \(1997, 2\)\)\)/
    sqls[1].should =~ /UPDATE hits SET (first = NULL, last = NULL|last = NULL, first = NULL) WHERE \(\(hits.first = 'F1'\) AND \(hits.last = 'L1'\) AND \(\(year, week\) NOT IN \(\(1997, 1\), \(1997, 2\)\)\)\)/
    sqls.length.should == 2
  end

  specify "should set associated right-side cpks correctly for left-side cpks for a many_to_many association" do
    @Vocalist.many_to_many :hits, :class=>@Hit, :join_table=>:vocalists_hits, :left_key=>[:first, :last], :right_key=>[:year, :week]
    @Vocalist.load(:first=>'F2', :last=>'L2').hit_pks = [[1997, 1], [1997, 2]]
    sqls = @db.sqls
    sqls[0].should == "DELETE FROM vocalists_hits WHERE (((first, last) IN (('F2', 'L2'))) AND ((year, week) NOT IN ((1997, 1), (1997, 2))))"
    sqls[1].should == "SELECT year, week FROM vocalists_hits WHERE ((first, last) IN (('F2', 'L2')))"
    match = sqls[2].match(/INSERT INTO vocalists_hits \((.*)\) VALUES \((.*)\)/)
    Hash[match[1].split(', ').zip(match[2].split(', '))].should == {"first"=>"'F2'", "last"=>"'L2'", "year"=>"1997", "week"=>"1"}
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
    sqls[1].should == "DELETE FROM albums_tags WHERE ((album_id IN ((2))) AND (tag_id NOT IN (1, 3)))"
    sqls[2].should == 'SELECT tag_id FROM albums_tags WHERE (album_id IN ((2)))'
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
    sqls[0].should == "DELETE FROM albums_tags WHERE ((album_id IN ((2))) AND (tag_id NOT IN (1, 3)))"
    sqls[1].should == 'SELECT tag_id FROM albums_tags WHERE (album_id IN ((2)))'
    sqls[2].should =~ /INSERT INTO albums_tags \((album_id, tag_id|tag_id, album_id)\) VALUES \((2, 1|1, 2)\)/
    sqls.length.should == 3
  end

  specify "should not automatically convert keys to numbers if the primary key is an integer for many_to_many associations" do
    @Tag.db_schema[:id][:type] = :string
    @Album.load(:id=>2).tag_pks = %w'1 3'
    sqls = @db.sqls
    sqls[0].should == "DELETE FROM albums_tags WHERE ((album_id IN ((2))) AND (tag_id NOT IN ('1', '3')))"
    sqls[1].should == 'SELECT tag_id FROM albums_tags WHERE (album_id IN ((2)))'
    sqls[2].should =~ /INSERT INTO albums_tags \((album_id, tag_id|tag_id, album_id)\) VALUES \((2, '1'|'1', 2)\)/
    sqls[3].should =~ /INSERT INTO albums_tags \((album_id, tag_id|tag_id, album_id)\) VALUES \((2, '3'|'3', 2)\)/
    sqls.length.should == 4
  end

end
