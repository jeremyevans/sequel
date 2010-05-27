require File.join(File.dirname(__FILE__), "spec_helper")

describe "Sequel::Plugins::AssociationPks" do
  before do
    @db = MODEL_DB.clone
    mod = Module.new do
      def fetch_rows(sql)
        case sql
        when "SELECT id FROM albums WHERE (albums.artist_id = 1)"
          yield({:id=>1})
          yield({:id=>2})
          yield({:id=>3})
        when /SELECT tag_id FROM albums_tags WHERE \(album_id = (\d)\)/
          yield({:tag_id=>1}) if $1 == '1'
          yield({:tag_id=>2}) if $1 != '3'
          yield({:tag_id=>3}) if $1 == '2'
        end
      end
    end
    @db.meta_def(:dataset) do |*opts|
      ds = super(*opts)
      ds.extend mod
      ds
    end
    def @db.transaction(opts)
      execute('BEGIN')
      yield
      execute('COMMIT')
    end
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
    @db.reset
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

  specify "should set associated pks correctly for a many_to_many association" do
    @Album.load(:id=>2).tag_pks = [1, 3]
    @db.sqls[0].should == "DELETE FROM albums_tags WHERE ((album_id = 2) AND (tag_id NOT IN (1, 3)))"
    @db.sqls[1].should =~ /INSERT INTO albums_tags \((album_id, tag_id|tag_id, album_id)\) VALUES \((2, 1|1, 2)\)/
  end

  specify "should use transactions if the object is configured to use transactions" do
    artist = @Artist.load(:id=>1)
    artist.use_transactions = true
    artist.album_pks = [1, 2]
    @db.sqls.should == ["BEGIN",
      "UPDATE albums SET artist_id = 1 WHERE (id IN (1, 2))",
      "UPDATE albums SET artist_id = NULL WHERE ((albums.artist_id = 1) AND (id NOT IN (1, 2)))",
      "COMMIT"]
    @db.reset

    album = @Album.load(:id=>2)
    album.use_transactions = true
    album.tag_pks = [1, 3]
    @db.sqls[0].should == "BEGIN"
    @db.sqls[1].should == "DELETE FROM albums_tags WHERE ((album_id = 2) AND (tag_id NOT IN (1, 3)))"
    @db.sqls[2].should =~ /INSERT INTO albums_tags \((album_id, tag_id|tag_id, album_id)\) VALUES \((2, 1|1, 2)\)/
    @db.sqls[3].should == "COMMIT"
  end

end
