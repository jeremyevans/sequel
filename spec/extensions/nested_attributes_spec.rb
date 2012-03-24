require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "NestedAttributes plugin" do
  def check_sqls(should, is)
    if should.is_a?(Array)
      should.should include(is)
    else
      should.should == is
    end
  end

  def check_sql_array(*shoulds)
    sqls = @db.sqls
    shoulds.length.should == sqls.length
    shoulds.zip(sqls){|s, i| check_sqls(s, i)}
  end

  before do
    @db = Sequel.mock(:autoid=>1, :numrows=>1)
    @c = Class.new(Sequel::Model(@db))
    @c.plugin :nested_attributes
    @Artist = Class.new(@c).set_dataset(:artists)
    @Album = Class.new(@c).set_dataset(:albums)
    @Tag = Class.new(@c).set_dataset(:tags)
    @Artist.plugin :skip_create_refresh
    @Album.plugin :skip_create_refresh
    @Tag.plugin :skip_create_refresh
    @Artist.columns :id, :name
    @Album.columns :id, :name, :artist_id
    @Tag.columns :id, :name
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id
    @Artist.one_to_one :first_album, :class=>@Album, :key=>:artist_id
    @Album.many_to_one :artist, :class=>@Artist
    @Album.many_to_many :tags, :class=>@Tag, :left_key=>:album_id, :right_key=>:tag_id, :join_table=>:at
    @Artist.nested_attributes :albums, :first_album, :destroy=>true, :remove=>true
    @Album.nested_attributes :artist, :tags, :destroy=>true, :remove=>true
    @db.sqls
  end
  
  it "should support creating new many_to_one objects" do
    a = @Album.new({:name=>'Al', :artist_attributes=>{:name=>'Ar'}})
    @db.sqls.should == []
    a.save
    check_sql_array("INSERT INTO artists (name) VALUES ('Ar')",
      ["INSERT INTO albums (name, artist_id) VALUES ('Al', 1)", "INSERT INTO albums (artist_id, name) VALUES (1, 'Al')"])
  end
  
  it "should support creating new one_to_one objects" do
    a = @Artist.new(:name=>'Ar')
    a.id = 1
    a.first_album_attributes = {:name=>'Al'}
    @db.sqls.should == []
    a.save
    check_sql_array(["INSERT INTO artists (name, id) VALUES ('Ar', 1)", "INSERT INTO artists (id, name) VALUES (1, 'Ar')"],
      "INSERT INTO albums (name) VALUES ('Al')",
      "UPDATE albums SET artist_id = NULL WHERE ((artist_id = 1) AND (id != 2))",
      ["UPDATE albums SET artist_id = 1, name = 'Al' WHERE (id = 2)", "UPDATE albums SET name = 'Al', artist_id = 1 WHERE (id = 2)"])
  end
  
  it "should support creating new one_to_many objects" do
    a = @Artist.new({:name=>'Ar', :albums_attributes=>[{:name=>'Al'}]})
    @db.sqls.should == []
    a.save
    check_sql_array("INSERT INTO artists (name) VALUES ('Ar')",
      ["INSERT INTO albums (artist_id, name) VALUES (1, 'Al')", "INSERT INTO albums (name, artist_id) VALUES ('Al', 1)"])
  end
  
  it "should support creating new many_to_many objects" do
    a = @Album.new({:name=>'Al', :tags_attributes=>[{:name=>'T'}]})
    @db.sqls.should == []
    a.save
    check_sql_array("INSERT INTO albums (name) VALUES ('Al')",
      "INSERT INTO tags (name) VALUES ('T')",
      ["INSERT INTO at (album_id, tag_id) VALUES (1, 2)", "INSERT INTO at (tag_id, album_id) VALUES (2, 1)"])
  end
  
  it "should add new objects to the cached association array as soon as the *_attributes= method is called" do
    a = @Artist.new({:name=>'Ar', :albums_attributes=>[{:name=>'Al', :tags_attributes=>[{:name=>'T'}]}]})
    a.albums.should == [@Album.new(:name=>'Al')]
    a.albums.first.tags.should == [@Tag.new(:name=>'T')]
  end
  
  it "should support updating many_to_one objects" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    al.associations[:artist] = ar
    al.set(:artist_attributes=>{:id=>'20', :name=>'Ar2'})
    @db.sqls.should == []
    al.save
    @db.sqls.should == ["UPDATE albums SET name = 'Al' WHERE (id = 10)", "UPDATE artists SET name = 'Ar2' WHERE (id = 20)"]
  end
  
  it "should support updating one_to_one objects" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    ar.associations[:first_album] = al
    ar.set(:first_album_attributes=>{:id=>10, :name=>'Al2'})
    @db.sqls.should == []
    ar.save
    @db.sqls.should == ["UPDATE artists SET name = 'Ar' WHERE (id = 20)", "UPDATE albums SET name = 'Al2' WHERE (id = 10)"]
  end
  
  it "should support updating one_to_many objects" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    ar.associations[:albums] = [al]
    ar.set(:albums_attributes=>[{:id=>10, :name=>'Al2'}])
    @db.sqls.should == []
    ar.save
    @db.sqls.should == ["UPDATE artists SET name = 'Ar' WHERE (id = 20)", "UPDATE albums SET name = 'Al2' WHERE (id = 10)"]
  end
  
  it "should support updating one_to_many objects with _delete/_remove flags set to false" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    ar.associations[:albums] = [al]
    ar.set(:albums_attributes=>[{:id=>10, :name=>'Al2', :_delete => 'f', :_remove => '0'}])
    @db.sqls.should == []
    ar.save
    @db.sqls.should == ["UPDATE artists SET name = 'Ar' WHERE (id = 20)", "UPDATE albums SET name = 'Al2' WHERE (id = 10)"]
  end
  
  it "should support updating many_to_many objects" do
    a = @Album.load(:id=>10, :name=>'Al')
    t = @Tag.load(:id=>20, :name=>'T')
    a.associations[:tags] = [t]
    a.set(:tags_attributes=>[{:id=>20, :name=>'T2'}])
    @db.sqls.should == []
    a.save
    @db.sqls.should == ["UPDATE albums SET name = 'Al' WHERE (id = 10)", "UPDATE tags SET name = 'T2' WHERE (id = 20)"]
  end
  
  it "should support updating many_to_many objects with _delete/_remove flags set to false" do
    a = @Album.load(:id=>10, :name=>'Al')
    t = @Tag.load(:id=>20, :name=>'T')
    a.associations[:tags] = [t]
    a.set(:tags_attributes=>[{:id=>20, :name=>'T2', '_delete' => false, '_remove' => 'F'}])
    @db.sqls.should == []
    a.save
    @db.sqls.should == ["UPDATE albums SET name = 'Al' WHERE (id = 10)", "UPDATE tags SET name = 'T2' WHERE (id = 20)"]
  end
  
  it "should support removing many_to_one objects" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    al.associations[:artist] = ar
    al.set(:artist_attributes=>{:id=>'20', :_remove=>'1'})
    @db.sqls.should == []
    al.save
    check_sql_array(["UPDATE albums SET artist_id = NULL, name = 'Al' WHERE (id = 10)", "UPDATE albums SET name = 'Al', artist_id = NULL WHERE (id = 10)"])
  end
  
  it "should support removing one_to_one objects" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    ar.associations[:first_album] = al
    ar.set(:first_album_attributes=>{:id=>10, :_remove=>'t'})
    @db.sqls.should == []
    ar.save
    @db.sqls.should == ["UPDATE albums SET artist_id = NULL WHERE (artist_id = 20)", "UPDATE artists SET name = 'Ar' WHERE (id = 20)"]
  end
  
  it "should support removing one_to_many objects" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    ar.associations[:albums] = [al]
    ar.set(:albums_attributes=>[{:id=>10, :_remove=>'t'}])
    @db.sqls.should == []
    @Album.dataset._fetch = {:id=>1}
    ar.save
    check_sql_array("SELECT 1 AS one FROM albums WHERE ((albums.artist_id = 20) AND (id = 10)) LIMIT 1",
      ["UPDATE albums SET artist_id = NULL, name = 'Al' WHERE (id = 10)", "UPDATE albums SET name = 'Al', artist_id = NULL WHERE (id = 10)"],
      "UPDATE artists SET name = 'Ar' WHERE (id = 20)")
  end
  
  it "should support removing many_to_many objects" do
    a = @Album.load(:id=>10, :name=>'Al')
    t = @Tag.load(:id=>20, :name=>'T')
    a.associations[:tags] = [t]
    a.set(:tags_attributes=>[{:id=>20, :_remove=>true}])
    @db.sqls.should == []
    a.save
    @db.sqls.should == ["DELETE FROM at WHERE ((album_id = 10) AND (tag_id = 20))", "UPDATE albums SET name = 'Al' WHERE (id = 10)"]
  end
  
  it "should support destroying many_to_one objects" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    al.associations[:artist] = ar
    al.set(:artist_attributes=>{:id=>'20', :_delete=>'1'})
    @db.sqls.should == []
    al.save
    check_sql_array(["UPDATE albums SET artist_id = NULL, name = 'Al' WHERE (id = 10)", "UPDATE albums SET name = 'Al', artist_id = NULL WHERE (id = 10)"],
      "DELETE FROM artists WHERE (id = 20)")
  end
  
  it "should support destroying one_to_one objects" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    ar.associations[:first_album] = al
    ar.set(:first_album_attributes=>{:id=>10, :_delete=>'t'})
    @db.sqls.should == []
    ar.save
    @db.sqls.should == ["UPDATE artists SET name = 'Ar' WHERE (id = 20)", "DELETE FROM albums WHERE (id = 10)"]
  end
  
  it "should support destroying one_to_many objects" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    ar.associations[:albums] = [al]
    ar.set(:albums_attributes=>[{:id=>10, :_delete=>'t'}])
    @db.sqls.should == []
    ar.save
    @db.sqls.should == ["UPDATE artists SET name = 'Ar' WHERE (id = 20)", "DELETE FROM albums WHERE (id = 10)"]
  end
  
  it "should support destroying many_to_many objects" do
    a = @Album.load(:id=>10, :name=>'Al')
    t = @Tag.load(:id=>20, :name=>'T')
    a.associations[:tags] = [t]
    a.set(:tags_attributes=>[{:id=>20, :_delete=>true}])
    @db.sqls.should == []
    a.save
    @db.sqls.should == ["DELETE FROM at WHERE ((album_id = 10) AND (tag_id = 20))", "UPDATE albums SET name = 'Al' WHERE (id = 10)", "DELETE FROM tags WHERE (id = 20)"]
  end
  
  it "should support both string and symbol keys in nested attribute hashes" do
    a = @Album.load(:id=>10, :name=>'Al')
    t = @Tag.load(:id=>20, :name=>'T')
    a.associations[:tags] = [t]
    a.set('tags_attributes'=>[{'id'=>20, '_delete'=>true}])
    @db.sqls.should == []
    a.save
    @db.sqls.should == ["DELETE FROM at WHERE ((album_id = 10) AND (tag_id = 20))", "UPDATE albums SET name = 'Al' WHERE (id = 10)", "DELETE FROM tags WHERE (id = 20)"]
  end
  
  it "should support using a hash instead of an array for to_many nested attributes" do
    a = @Album.load(:id=>10, :name=>'Al')
    t = @Tag.load(:id=>20, :name=>'T')
    a.associations[:tags] = [t]
    a.set('tags_attributes'=>{'1'=>{'id'=>20, '_delete'=>true}})
    @db.sqls.should == []
    a.save
    @db.sqls.should == ["DELETE FROM at WHERE ((album_id = 10) AND (tag_id = 20))", "UPDATE albums SET name = 'Al' WHERE (id = 10)", "DELETE FROM tags WHERE (id = 20)"]
  end
  
  it "should only allow destroying associated objects if :destroy option is used in the nested_attributes call" do
    a = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    a.associations[:artist] = ar
    @Album.nested_attributes :artist
    proc{a.set(:artist_attributes=>{:id=>'20', :_delete=>'1'})}.should raise_error(Sequel::Error)
    @Album.nested_attributes :artist, :destroy=>true
    proc{a.set(:artist_attributes=>{:id=>'20', :_delete=>'1'})}.should_not raise_error(Sequel::Error)
  end
  
  it "should only allow removing associated objects if :remove option is used in the nested_attributes call" do
    a = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    a.associations[:artist] = ar
    @Album.nested_attributes :artist
    proc{a.set(:artist_attributes=>{:id=>'20', :_remove=>'1'})}.should raise_error(Sequel::Error)
    @Album.nested_attributes :artist, :remove=>true
    proc{a.set(:artist_attributes=>{:id=>'20', :_remove=>'1'})}.should_not raise_error(Sequel::Error)
  end
  
  it "should raise an Error if a primary key is given in a nested attribute hash, but no matching associated object exists" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    ar.associations[:albums] = [al]
    proc{ar.set(:albums_attributes=>[{:id=>30, :_delete=>'t'}])}.should raise_error(Sequel::Error)
    proc{ar.set(:albums_attributes=>[{:id=>10, :_delete=>'t'}])}.should_not raise_error(Sequel::Error)
  end
  
  it "should not raise an Error if an unmatched primary key is given, if the :strict=>false option is used" do
    @Artist.nested_attributes :albums, :strict=>false
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    ar.associations[:albums] = [al]
    ar.set(:albums_attributes=>[{:id=>30, :_delete=>'t'}])
    @db.sqls.should == []
    ar.save
    @db.sqls.should == ["UPDATE artists SET name = 'Ar' WHERE (id = 20)"]
  end
  
  it "should not save if nested attribute is not valid and should include nested attribute validation errors in the main object's validation errors" do
    @Artist.class_eval do
      def validate
        super
        errors.add(:name, 'cannot be Ar') if name == 'Ar'
      end
    end
    a = @Album.new(:name=>'Al', :artist_attributes=>{:name=>'Ar'})
    @db.sqls.should == []
    proc{a.save}.should raise_error(Sequel::ValidationFailed)
    a.errors.full_messages.should == ['artist name cannot be Ar']
    @db.sqls.should == []
    # Should preserve attributes
    a.artist.name.should == 'Ar'
  end
  
  it "should not attempt to validate nested attributes if the :validate=>false association option is used" do
    @Album.many_to_one :artist, :class=>@Artist, :validate=>false
    @Album.nested_attributes :artist, :tags, :destroy=>true, :remove=>true
    @Artist.class_eval do
      def validate
        super
        errors.add(:name, 'cannot be Ar') if name == 'Ar'
      end
    end
    a = @Album.new(:name=>'Al', :artist_attributes=>{:name=>'Ar'})
    @db.sqls.should == []
    a.save
    check_sql_array("INSERT INTO artists (name) VALUES ('Ar')",
      ["INSERT INTO albums (artist_id, name) VALUES (1, 'Al')", "INSERT INTO albums (name, artist_id) VALUES ('Al', 1)"])
  end
  
  it "should not attempt to validate nested attributes if the :validate=>false option is passed to save" do
    @Artist.class_eval do
      def validate
        super
        errors.add(:name, 'cannot be Ar') if name == 'Ar'
      end
    end
    a = @Album.new(:name=>'Al', :artist_attributes=>{:name=>'Ar'})
    @db.sqls.should == []
    a.save(:validate=>false)
    check_sql_array("INSERT INTO artists (name) VALUES ('Ar')",
      ["INSERT INTO albums (artist_id, name) VALUES (1, 'Al')", "INSERT INTO albums (name, artist_id) VALUES ('Al', 1)"])
  end
  
  it "should not accept nested attributes unless explicitly specified" do
    @Artist.many_to_many :tags, :class=>@Tag, :left_key=>:album_id, :right_key=>:tag_id, :join_table=>:at
    proc{@Artist.create({:name=>'Ar', :tags_attributes=>[{:name=>'T'}]})}.should raise_error(Sequel::Error)
    @db.sqls.should == []
  end
  
  it "should save when save_changes or update is called if nested attribute associated objects changed but there are no changes to the main object" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    al.associations[:artist] = ar
    @db.sqls.should == []
    al.update(:artist_attributes=>{:id=>'20', :name=>'Ar2'})
    @db.sqls.should == ["UPDATE artists SET name = 'Ar2' WHERE (id = 20)"]
  end
  
  it "should have a :limit option limiting the amount of entries" do
    @Album.nested_attributes :tags, :limit=>2
    arr = [{:name=>'T'}]
    proc{@Album.new({:name=>'Al', :tags_attributes=>arr*3})}.should raise_error(Sequel::Error)
    a = @Album.new({:name=>'Al', :tags_attributes=>arr*2})
    @db.sqls.should == []
    a.save
    check_sql_array("INSERT INTO albums (name) VALUES ('Al')",
      "INSERT INTO tags (name) VALUES ('T')",
      ["INSERT INTO at (album_id, tag_id) VALUES (1, 2)", "INSERT INTO at (tag_id, album_id) VALUES (2, 1)"],
      "INSERT INTO tags (name) VALUES ('T')",
      ["INSERT INTO at (album_id, tag_id) VALUES (1, 4)", "INSERT INTO at (tag_id, album_id) VALUES (4, 1)"])
  end
  
  it "should accept a block that each hash gets passed to determine if it should be processed" do
    @Album.nested_attributes(:tags){|h| h[:name].empty?}
    a = @Album.new({:name=>'Al', :tags_attributes=>[{:name=>'T'}, {:name=>''}, {:name=>'T2'}]})
    @db.sqls.should == []
    a.save
    check_sql_array("INSERT INTO albums (name) VALUES ('Al')",
      "INSERT INTO tags (name) VALUES ('T')",
      ["INSERT INTO at (album_id, tag_id) VALUES (1, 2)", "INSERT INTO at (tag_id, album_id) VALUES (2, 1)"],
      "INSERT INTO tags (name) VALUES ('T2')",
      ["INSERT INTO at (album_id, tag_id) VALUES (1, 4)", "INSERT INTO at (tag_id, album_id) VALUES (4, 1)"])
  end
  
  it "should return objects created/modified in the internal methods" do
    @Album.nested_attributes :tags, :remove=>true, :strict=>false
    objs = []
    @Album.class_eval do
      define_method(:nested_attributes_create){|*a| objs << [super(*a), :create]}
      define_method(:nested_attributes_remove){|*a| objs << [super(*a), :remove]}
      define_method(:nested_attributes_update){|*a| objs << [super(*a), :update]}
    end
    a = @Album.new(:name=>'Al')
    a.associations[:tags] = [@Tag.load(:id=>6, :name=>'A'), @Tag.load(:id=>7, :name=>'A2')] 
    a.tags_attributes = [{:id=>6, :name=>'T'}, {:id=>7, :name=>'T2', :_remove=>true}, {:name=>'T3'}, {:id=>8, :name=>'T4'}, {:id=>9, :name=>'T5', :_remove=>true}]
    objs.should == [[@Tag.load(:id=>6, :name=>'T'), :update], [@Tag.load(:id=>7, :name=>'A2'), :remove], [@Tag.new(:name=>'T3'), :create], [nil, :update], [nil, :remove]]
  end

  it "should raise an error if updating modifies the associated objects keys" do
    @Artist.columns :id, :name, :artist_id
    @Album.columns :id, :name, :artist_id
    @Tag.columns :id, :name, :tag_id
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id, :primary_key=>:artist_id
    @Album.many_to_one :artist, :class=>@Artist, :primary_key=>:artist_id
    @Album.many_to_many :tags, :class=>@Tag, :left_key=>:album_id, :right_key=>:tag_id, :join_table=>:at, :right_primary_key=>:tag_id
    @Artist.nested_attributes :albums, :destroy=>true, :remove=>true
    @Album.nested_attributes :artist, :tags, :destroy=>true, :remove=>true

    al = @Album.load(:id=>10, :name=>'Al', :artist_id=>25)
    ar = @Artist.load(:id=>20, :name=>'Ar', :artist_id=>25)
    t = @Tag.load(:id=>30, :name=>'T', :tag_id=>15)
    al.associations[:artist] = ar
    al.associations[:tags] = [t]
    ar.associations[:albums] = [al]
    proc{ar.set(:albums_attributes=>[{:id=>10, :name=>'Al2', :artist_id=>'3'}])}.should raise_error(Sequel::Error)
    proc{al.set(:artist_attributes=>{:id=>20, :name=>'Ar2', :artist_id=>'3'})}.should raise_error(Sequel::Error)
    proc{al.set(:tags_attributes=>[{:id=>30, :name=>'T2', :tag_id=>'3'}])}.should raise_error(Sequel::Error)
  end

  it "should accept a :fields option and only allow modification of those fields" do
    @Tag.columns :id, :name, :number
    @Album.nested_attributes :tags, :destroy=>true, :remove=>true, :fields=>[:name]

    al = @Album.load(:id=>10, :name=>'Al')
    t = @Tag.load(:id=>30, :name=>'T', :number=>10)
    al.associations[:tags] = [t]
    al.set(:tags_attributes=>[{:id=>30, :name=>'T2'}, {:name=>'T3'}])
    @db.sqls.should == []
    al.save
    check_sql_array("UPDATE albums SET name = 'Al' WHERE (id = 10)",
      "UPDATE tags SET name = 'T2' WHERE (id = 30)",
      "INSERT INTO tags (name) VALUES ('T3')",
      ["INSERT INTO at (album_id, tag_id) VALUES (10, 1)", "INSERT INTO at (tag_id, album_id) VALUES (1, 10)"])
    proc{al.set(:tags_attributes=>[{:id=>30, :name=>'T2', :number=>3}])}.should raise_error(Sequel::Error)
    proc{al.set(:tags_attributes=>[{:name=>'T2', :number=>3}])}.should raise_error(Sequel::Error)
  end
end
