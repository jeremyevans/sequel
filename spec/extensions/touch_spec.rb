require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Touch plugin" do
  before do
    @c = Class.new(Sequel::Model)
    p = proc{def touch_instance_value; touch_association_value; end}
    @Artist = Class.new(@c, &p).set_dataset(:artists)
    @Album = Class.new(@c, &p).set_dataset(:albums)

    @Artist.columns :id, :updated_at, :modified_on
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id

    @Album.columns :id, :updated_at, :modified_on, :artist_id, :original_album_id
    @Album.one_to_many :followup_albums, :class=>@Album, :key=>:original_album_id
    @Album.many_to_one :artist, :class=>@Artist

    @a = @Artist.load(:id=>1)
    MODEL_DB.reset
  end

  specify "should default to using Time.now when setting the column values for model instances" do
    c = Class.new(Sequel::Model).set_dataset(:a)
    c.plugin :touch
    c.columns :id, :updated_at
    c.load(:id=>1).touch
    MODEL_DB.sqls.first.should =~ /UPDATE a SET updated_at = '[-0-9 :.]+' WHERE \(id = 1\)/
  end

  specify "should allow #touch instance method for updating the updated_at column" do
    @Artist.plugin :touch
    @a.touch
    MODEL_DB.sqls.should == ["UPDATE artists SET updated_at = CURRENT_TIMESTAMP WHERE (id = 1)"]
  end

  specify "should have #touch take an argument for the column to touch" do
    @Artist.plugin :touch
    @a.touch(:modified_on)
    MODEL_DB.sqls.should == ["UPDATE artists SET modified_on = CURRENT_TIMESTAMP WHERE (id = 1)"]
  end

  specify "should be able to specify the default column to touch in the plugin call using the :column option" do
    @Artist.plugin :touch, :column=>:modified_on
    @a.touch
    MODEL_DB.sqls.should == ["UPDATE artists SET modified_on = CURRENT_TIMESTAMP WHERE (id = 1)"]
  end

  specify "should be able to specify the default column to touch using the touch_column model accessor" do
    @Artist.plugin :touch
    @Artist.touch_column = :modified_on
    @a.touch
    MODEL_DB.sqls.should == ["UPDATE artists SET modified_on = CURRENT_TIMESTAMP WHERE (id = 1)"]
  end

  specify "should be able to specify the associations to touch in the plugin call using the :associations option" do
    @Artist.plugin :touch, :associations=>:albums
    @a.touch
    MODEL_DB.sqls.should == ["UPDATE artists SET updated_at = CURRENT_TIMESTAMP WHERE (id = 1)",
      "UPDATE albums SET updated_at = CURRENT_TIMESTAMP WHERE (albums.artist_id = 1)"]
  end

  specify "should be able to give an array to the :associations option specifying multiple associations" do
    @Album.plugin :touch, :associations=>[:artist, :followup_albums]
    @Album.load(:id=>4, :artist_id=>1).touch
    sqls = MODEL_DB.sqls
    sqls.shift.should == "UPDATE albums SET updated_at = CURRENT_TIMESTAMP WHERE (id = 4)"
    sqls.sort.should == ["UPDATE albums SET updated_at = CURRENT_TIMESTAMP WHERE (albums.original_album_id = 4)",
      "UPDATE artists SET updated_at = CURRENT_TIMESTAMP WHERE (artists.id = 1)"]
  end

  specify "should be able to give a hash to the :associations option specifying the column to use for each association" do
    @Artist.plugin :touch, :associations=>{:albums=>:modified_on}
    @a.touch
    MODEL_DB.sqls.should == ["UPDATE artists SET updated_at = CURRENT_TIMESTAMP WHERE (id = 1)",
      "UPDATE albums SET modified_on = CURRENT_TIMESTAMP WHERE (albums.artist_id = 1)"]
  end

  specify "should default to using the touch_column as the default touch column for associations" do
    @Artist.plugin :touch, :column=>:modified_on, :associations=>:albums
    @a.touch
    MODEL_DB.sqls.should == ["UPDATE artists SET modified_on = CURRENT_TIMESTAMP WHERE (id = 1)",
      "UPDATE albums SET modified_on = CURRENT_TIMESTAMP WHERE (albums.artist_id = 1)"]
  end

  specify "should allow the mixed use of symbols and hashes inside an array for the :associations option" do
    @Album.plugin :touch, :associations=>[:artist, {:followup_albums=>:modified_on}]
    @Album.load(:id=>4, :artist_id=>1).touch
    sqls = MODEL_DB.sqls
    sqls.shift.should == "UPDATE albums SET updated_at = CURRENT_TIMESTAMP WHERE (id = 4)"
    sqls.sort.should == ["UPDATE albums SET modified_on = CURRENT_TIMESTAMP WHERE (albums.original_album_id = 4)",
      "UPDATE artists SET updated_at = CURRENT_TIMESTAMP WHERE (artists.id = 1)"]
  end

  specify "should be able to specify the associations to touch via a touch_associations_method" do
    @Album.plugin :touch
    @Album.touch_associations(:artist, {:followup_albums=>:modified_on})
    @Album.load(:id=>4, :artist_id=>1).touch
    sqls = MODEL_DB.sqls
    sqls.shift.should == "UPDATE albums SET updated_at = CURRENT_TIMESTAMP WHERE (id = 4)"
    sqls.sort.should == ["UPDATE albums SET modified_on = CURRENT_TIMESTAMP WHERE (albums.original_album_id = 4)",
      "UPDATE artists SET updated_at = CURRENT_TIMESTAMP WHERE (artists.id = 1)"]
  end

  specify "should touch associated objects when destroying an object" do
    @Album.plugin :touch
    @Album.touch_associations(:artist, {:followup_albums=>:modified_on})
    @Album.load(:id=>4, :artist_id=>1).destroy
    sqls = MODEL_DB.sqls
    sqls.shift.should == "DELETE FROM albums WHERE (id = 4)"
    sqls.sort.should == ["UPDATE albums SET modified_on = CURRENT_TIMESTAMP WHERE (albums.original_album_id = 4)",
      "UPDATE artists SET updated_at = CURRENT_TIMESTAMP WHERE (artists.id = 1)"]
  end

  specify "should not update a column that doesn't exist" do
    @Album.plugin :touch, :column=>:x
    a = @Album.load(:id=>1)
    a.touch
    MODEL_DB.sqls.should == []
    a.artist_id = 1
    a.touch
    MODEL_DB.sqls.should == ['UPDATE albums SET artist_id = 1 WHERE (id = 1)']
  end

  specify "should raise an error if given a column argument in touch that doesn't exist" do
    @Artist.plugin :touch
    proc{@a.touch(:x)}.should raise_error(Sequel::Error)
  end

  specify "should raise an Error when a nonexistent association is given" do
    @Artist.plugin :touch
    proc{@Artist.plugin :touch, :associations=>:blah}.should raise_error(Sequel::Error)
  end

  specify "should work correctly in subclasses" do
    @Artist.plugin :touch
    c1 = Class.new(@Artist)
    c1.load(:id=>4).touch
    MODEL_DB.sqls.should == ["UPDATE artists SET updated_at = CURRENT_TIMESTAMP WHERE (id = 4)"]

    c1.touch_column = :modified_on
    c1.touch_associations :albums
    c1.load(:id=>1).touch
    MODEL_DB.sqls.should == ["UPDATE artists SET modified_on = CURRENT_TIMESTAMP WHERE (id = 1)",
      "UPDATE albums SET modified_on = CURRENT_TIMESTAMP WHERE (albums.artist_id = 1)"]

    @a.touch
    MODEL_DB.sqls.should == ["UPDATE artists SET updated_at = CURRENT_TIMESTAMP WHERE (id = 1)"]

    @Artist.plugin :touch, :column=>:modified_on, :associations=>:albums
    c2 = Class.new(@Artist)
    c2.load(:id=>4).touch
    MODEL_DB.sqls.should == ["UPDATE artists SET modified_on = CURRENT_TIMESTAMP WHERE (id = 4)",
      "UPDATE albums SET modified_on = CURRENT_TIMESTAMP WHERE (albums.artist_id = 4)"]
  end
end
