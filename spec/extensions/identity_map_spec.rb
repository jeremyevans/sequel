require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::IdentityMap" do
  before do
    class ::IdentityMapModel < Sequel::Model
      plugin :identity_map
      attr_accessor :foo
      dataset._fetch = proc do |sql|
        sql =~ /WHERE \(?(\w+\.)?(\w+) = (\d)\)?/
        {$2.to_sym=>$3.to_i}
      end
      def self.waw_identity_map(&block) # with and without
        with_identity_map(&block)
        db.reset
        yield
      end
    end
    class ::IdentityMapAlbum < ::IdentityMapModel
      columns :artist_id
    end
    class ::IdentityMapArtist < ::IdentityMapModel
    end
    @c = ::IdentityMapModel
    @c1 = ::IdentityMapAlbum
    @c2 = ::IdentityMapArtist
    MODEL_DB.reset
  end
  after do
    Object.send(:remove_const, :IdentityMapAlbum)
    Object.send(:remove_const, :IdentityMapArtist)
    Object.send(:remove_const, :IdentityMapModel)
  end

  it "#identity_map should return a hash if an identity map is currently being used" do
    @c.with_identity_map{@c.identity_map.should == {}}
  end

  it "#identity_map should return nil if an identity map is not currently being used" do
    @c.identity_map.should == nil
  end

  it "#identity_map_key should be the same for the same class and pk" do
    @c.identity_map_key(1).should == @c.identity_map_key(1)
  end

  it "#identity_map_key should be different for a different class" do
    @c1.identity_map_key(1).should_not == @c2.identity_map_key(1)
  end

  it "#identity_map_key should be different for different anonymous classes" do
    Class.new(@c).identity_map_key(1).should_not == Class.new(@c).identity_map_key(1)
  end

  it "#identity_map_key should be different for a different pk" do
    @c.identity_map_key(1).should_not == @c.identity_map_key(2)
  end

  it "#identity_map_key should be nil for an empty pk values" do
    @c.identity_map_key(nil).should == nil
    @c.identity_map_key([]).should == nil
    @c.identity_map_key([nil]).should == nil
  end

  it "#load should work even if model doesn't have a primary key" do
    c = Class.new(@c)
    c.no_primary_key
    proc{c.with_identity_map{c.load({})}}.should_not raise_error
    c.with_identity_map{c.load({}).should_not equal(c.load({}))}
  end

  it "#load should return an object if there is no current identity map" do
    o = @c.load(:id=>1)
    o.should be_a_kind_of(@c)
    o.values.should == {:id=>1}
  end

  it "#load should return an object if there is a current identity map" do
    @c.with_identity_map do
      o = @c.load(:id=>1)
      o.should be_a_kind_of(@c)
      o.values.should == {:id=>1}
    end
  end

  it "#load should store the object in the current identity map if it isn't already there" do
    @c.with_identity_map do
      @c.identity_map[@c.identity_map_key(1)].should == nil
      o = @c.load(:id=>1)
      @c.identity_map[@c.identity_map_key(1)].should == o
    end
  end

  it "#load should update the record in the current identity map if new fields if it is already there" do
    @c.with_identity_map do
      o = @c.load(:id=>1, :a=>2)
      o.values.should == {:id=>1, :a=>2}
      o = @c.load(:id=>1, :b=>3)
      o.values.should == {:id=>1, :a=>2, :b=>3}
    end
  end

  it "#load should not update existing fields in the record if the record is in the current identity map" do
    @c.with_identity_map do
      o = @c.load(:id=>1, :a=>2)
      o.values.should == {:id=>1, :a=>2}
      o = @c.load(:id=>1, :a=>4)
      o.values.should == {:id=>1, :a=>2}
    end
  end

  it "should use the identity map as a lookup cache in Model.[] to save on database queries" do
    @c.with_identity_map do
      MODEL_DB.sqls.length.should == 0
      o = @c[1]
      MODEL_DB.sqls.length.should == 1
      o.foo = 1
      @c[1].foo.should == o.foo
      MODEL_DB.sqls.length.should == 0
      @c[2].foo.should_not == o.foo
      MODEL_DB.sqls.length.should == 1
    end
  end

  it "should remove instances from the identity map if they are deleted or destroyed" do
    @c.with_identity_map do
      MODEL_DB.sqls.length.should == 0
      o = @c[1]
      MODEL_DB.sqls.length.should == 1
      o.foo = 1
      @c[1].should == o
      MODEL_DB.sqls.length.should == 0
      o.destroy
      MODEL_DB.sqls.length.should == 1
      @c[1].foo.should_not == o.foo
      MODEL_DB.sqls.length.should == 1

      MODEL_DB.reset
      o = @c[2]
      MODEL_DB.sqls.length.should == 1
      o.foo = 1
      @c[2].should == o
      MODEL_DB.sqls.length.should == 0
      o.delete
      MODEL_DB.sqls.length.should == 1
      @c[2].foo.should_not == o.foo
      MODEL_DB.sqls.length.should == 1
    end
  end

  it "should use the identity map as a lookup cache when retrieving many_to_one associated records via a composite key" do
    @c1.columns :another_id
    @c1.many_to_one :artist, :class=>@c2, :key=>[:id, :another_id]
    @c.with_identity_map do
      MODEL_DB.sqls.length.should == 0
      o = @c1.load(:id=>1, :another_id=>1, :artist_id=>2)
      a = o.artist
      a.should be_a_kind_of(@c2)
      MODEL_DB.sqls.length.should == 1
      o = @c1.load(:id=>1, :another_id=>2, :artist_id=>2)
      o.artist.should == a
      MODEL_DB.sqls.length.should == 0
      o = @c1.load(:id=>3, :another_id=>3, :artist_id=>3)
      o.artist.should_not == a
      MODEL_DB.sqls.length.should == 1
    end
  end

  it "should use the identity map as a lookup cache when retrieving many_to_one associated records" do
    @c1.many_to_one :artist, :class=>@c2
    @c.with_identity_map do
      MODEL_DB.sqls.length.should == 0
      o = @c1.load(:id=>1, :artist_id=>2)
      a = o.artist
      a.should be_a_kind_of(@c2)
      MODEL_DB.sqls.length.should == 1
      o = @c1.load(:id=>2, :artist_id=>2)
      o.artist.should == a
      MODEL_DB.sqls.length.should == 0
      o = @c1.load(:id=>3, :artist_id=>3)
      o.artist.should_not == a
      MODEL_DB.sqls.length.should == 1
    end
  end

  it "should not use the identity map as a lookup cache for a one_to_one association" do
    c = @c2
    @c2.one_to_one :artist, :class=>@c1, :key=>:artist_id
    @c.with_identity_map do
      MODEL_DB.sqls.length.should == 0
      o = @c2.load(:id=>2)
      a = o.artist
      a.should be_a_kind_of(@c1)
      MODEL_DB.sqls.length.should == 1
      o.reload
      MODEL_DB.sqls.length.should == 1
      o.artist.should == a
      MODEL_DB.sqls.length.should == 1
    end
  end

  it "should not use the identity map as a lookup cache if the assocation has a nil :key option" do
    c = @c2
    @c1.many_to_one :artist, :class=>@c2, :key=>nil, :dataset=>proc{c.filter(:artist_id=>artist_id)}
    @c.with_identity_map do
      MODEL_DB.sqls.length.should == 0
      o = @c1.load(:id=>1, :artist_id=>2)
      a = o.artist
      a.should be_a_kind_of(@c2)
      MODEL_DB.sqls.length.should == 1
      o = @c1.load(:id=>2, :artist_id=>2)
      o.artist.should == a
      MODEL_DB.sqls.length.should == 1
    end
  end

  it "should not use the identity map as a lookup cache if the assocation's :primary_key option doesn't match the primary key of the associated class" do
    @c1.many_to_one :artist, :class=>@c2, :primary_key=>:artist_id
    @c.with_identity_map do
      MODEL_DB.sqls.length.should == 0
      o = @c1.load(:id=>1, :artist_id=>2)
      a = o.artist
      a.should be_a_kind_of(@c2)
      MODEL_DB.sqls.length.should == 1
      o = @c1.load(:id=>2, :artist_id=>2)
      o.artist.should == a
      MODEL_DB.sqls.length.should == 1
    end
  end

  it "should not use the identity map as a lookup cache if a dynamic callback is used" do
    @c1.many_to_one :artist, :class=>@c2
    @c.with_identity_map do
      MODEL_DB.sqls.length.should == 0
      o = @c1.load(:id=>1, :artist_id=>2)
      a = o.artist
      a.should be_a_kind_of(@c2)
      MODEL_DB.sqls.length.should == 1
      o = @c1.load(:id=>2, :artist_id=>2)
      o.artist.should == a
      MODEL_DB.sqls.length.should == 0
      o = @c1.load(:id=>3, :artist_id=>3)
      o.artist.should_not == a
      MODEL_DB.sqls.length.should == 1
    end
  end

  it "should not override custom :eager_loaders for many_to_many associations" do
    @c1.columns :id
    @c2.columns :id
    c = @c2
    @c1.many_to_many :artists, :class=>@c2, :left_key=>:album_id, :right_key=>:artist_id, :join_table=>:aa, :eager_loader=>(proc do |eo|
       eo[:rows].each{|object| object.associations[:artists] = [c.load(:id=>object.id)]}
    end)
    @c1.dataset._fetch = [{:id=>1}, {:id=>2}, {:id=>3}]

    @c.waw_identity_map do
      MODEL_DB.sqls.length.should == 0
      a = @c1.eager(:artists).all
      MODEL_DB.sqls.length.should == 1
      a.should == [@c1.load(:id=>1), @c1.load(:id=>2), @c1.load(:id=>3)]
      a.map{|x| x.artists}.should == [[@c2.load(:id=>1)], [@c2.load(:id=>2)], [@c2.load(:id=>3)]]
      MODEL_DB.sqls.length.should == 0
    end
  end

  it "should work correctly when eagerly loading many_to_many associations" do
    @c1.columns :id
    @c2.columns :id
    @c1.many_to_many :artists, :class=>@c2, :left_key=>:album_id, :right_key=>:artist_id, :join_table=>:aa
    @c1.dataset._fetch = [{:id=>1}, {:id=>2}, {:id=>3}]
    @c2.dataset._fetch = [{:id=>1, :x_foreign_key_x=>1}, {:id=>1, :x_foreign_key_x=>2}, {:id=>2, :x_foreign_key_x=>1}, {:id=>2, :x_foreign_key_x=>2}, {:id=>3, :x_foreign_key_x=>1}, {:id=>3, :x_foreign_key_x=>1}]

    @c.waw_identity_map do
      MODEL_DB.sqls.length.should == 0
      a = @c1.eager(:artists).all
      MODEL_DB.sqls.length.should == 2
      a.should == [@c1.load(:id=>1), @c1.load(:id=>2), @c1.load(:id=>3)]
      a.map{|x| x.artists}.should == [[@c2.load(:id=>1), @c2.load(:id=>2), @c2.load(:id=>3), @c2.load(:id=>3)], [@c2.load(:id=>1), @c2.load(:id=>2)], []]
      MODEL_DB.sqls.length.should == 0
    end
  end

  it "should work correctly when eagerly loading many_to_many associations with composite keys" do
    @c1.columns :id, :id2
    @c2.columns :id, :id2
    @c1.set_primary_key :id, :id2
    @c2.set_primary_key :id, :id2
    @c1.many_to_many :artists, :class=>@c2, :left_key=>[:album_id1, :album_id2], :right_key=>[:artist_id1, :artist_id2], :join_table=>:aa
    @c1.dataset._fetch = [{:id=>1, :id2=>4}, {:id=>2, :id2=>5}, {:id=>3, :id2=>6}]
    @c2.dataset._fetch = [ {:id=>1, :x_foreign_key_0_x=>1, :x_foreign_key_1_x=>4}, {:id=>1, :x_foreign_key_0_x=>2, :x_foreign_key_1_x=>5}, {:id=>2, :x_foreign_key_0_x=>1, :x_foreign_key_1_x=>4}, {:id=>2, :x_foreign_key_0_x=>2, :x_foreign_key_1_x=>5}, {:id=>3, :x_foreign_key_0_x=>1, :x_foreign_key_1_x=>4}, {:id=>3, :x_foreign_key_0_x=>1, :x_foreign_key_1_x=>4}]

    @c.waw_identity_map do
      MODEL_DB.sqls.length.should == 0
      a = @c1.eager(:artists).all
      MODEL_DB.sqls.length.should == 2
      a.should == [@c1.load(:id=>1, :id2=>4), @c1.load(:id=>2, :id2=>5), @c1.load(:id=>3, :id2=>6)]
      a.map{|x| x.artists}.should == [[@c2.load(:id=>1), @c2.load(:id=>2), @c2.load(:id=>3), @c2.load(:id=>3)], [@c2.load(:id=>1), @c2.load(:id=>2)], []]
      MODEL_DB.sqls.length.should == 0
    end
  end

  it "should work correctly when eagerly loading many_through_many associations" do
    @c1.columns :id
    @c2.columns :id
    @c1.plugin :many_through_many
    @c1.many_through_many :artists, [[:aa, :album_id, :artist_id]], :class=>@c2
    @c1.dataset._fetch = [{:id=>1}, {:id=>2}, {:id=>3}]
    @c2.dataset._fetch = [{:id=>1, :x_foreign_key_x=>1}, {:id=>1, :x_foreign_key_x=>2}, {:id=>2, :x_foreign_key_x=>1}, {:id=>2, :x_foreign_key_x=>2}, {:id=>3, :x_foreign_key_x=>1}, {:id=>3, :x_foreign_key_x=>1}]

    @c.waw_identity_map do
      MODEL_DB.sqls.length.should == 0
      a = @c1.eager(:artists).all
      MODEL_DB.sqls.length.should == 2
      a.should == [@c1.load(:id=>1), @c1.load(:id=>2), @c1.load(:id=>3)]
      a.map{|x| x.artists}.should == [[@c2.load(:id=>1), @c2.load(:id=>2), @c2.load(:id=>3), @c2.load(:id=>3)], [@c2.load(:id=>1), @c2.load(:id=>2)], []]
      MODEL_DB.sqls.length.should == 0
    end
  end

  it "should work correctly when eagerly loading many_to_many associations with composite keys" do
    @c1.columns :id, :id2
    @c2.columns :id
    @c1.set_primary_key :id, :id2
    @c1.plugin :many_through_many
    @c1.many_through_many :artists, [[:aa, [:album_id1, :album_id2], :artist_id]], :class=>@c2
    @c1.dataset._fetch = [{:id=>1, :id2=>4}, {:id=>2, :id2=>5}, {:id=>3, :id2=>6}]
    @c2.dataset._fetch = [ {:id=>1, :x_foreign_key_0_x=>1, :x_foreign_key_1_x=>4}, {:id=>1, :x_foreign_key_0_x=>2, :x_foreign_key_1_x=>5}, {:id=>2, :x_foreign_key_0_x=>1, :x_foreign_key_1_x=>4}, {:id=>2, :x_foreign_key_0_x=>2, :x_foreign_key_1_x=>5}, {:id=>3, :x_foreign_key_0_x=>1, :x_foreign_key_1_x=>4}, {:id=>3, :x_foreign_key_0_x=>1, :x_foreign_key_1_x=>4}]

    @c.waw_identity_map do
      MODEL_DB.sqls.length.should == 0
      a = @c1.eager(:artists).all
      MODEL_DB.sqls.length.should == 2
      a.should == [@c1.load(:id=>1, :id2=>4), @c1.load(:id=>2, :id2=>5), @c1.load(:id=>3, :id2=>6)]
      a.map{|x| x.artists}.should == [[@c2.load(:id=>1), @c2.load(:id=>2), @c2.load(:id=>3), @c2.load(:id=>3)], [@c2.load(:id=>1), @c2.load(:id=>2)], []]
      MODEL_DB.sqls.length.should == 0
    end
  end
end
