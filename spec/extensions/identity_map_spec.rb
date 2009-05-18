require File.join(File.dirname(__FILE__), "spec_helper")

describe "Sequel::Plugins::IdentityMap" do
  before do
    class ::IdentityMapModel < Sequel::Model
      plugin :identity_map
      columns :id
      ds = dataset
      def ds.fetch_rows(sql)
        c = @opts[:where].args.first
        c = c.column if c.is_a?(Sequel::SQL::QualifiedIdentifier)
        h = {c=>@opts[:where].args.last}
        execute(sql)
        yield h
      end
    end
    class ::IdentityMapAlbum < ::IdentityMapModel
      columns :id, :artist_id
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

  it "#identity_map_key should be different if the pk is nil" do
    @c.identity_map_key(nil).should_not == @c.identity_map_key(nil)
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

  it "#load should should store the object in the current identity map if it isn't already there" do
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
      @c[1].should == o
      MODEL_DB.sqls.length.should == 1
      @c[2].should_not == o
      MODEL_DB.sqls.length.should == 2
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
      MODEL_DB.sqls.length.should == 1
      o = @c1.load(:id=>3, :artist_id=>3)
      o.artist.should_not == a
      MODEL_DB.sqls.length.should == 2
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
      MODEL_DB.sqls.length.should == 2
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
      MODEL_DB.sqls.length.should == 2
    end
  end
end
