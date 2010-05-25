require File.join(File.dirname(__FILE__), "spec_helper")

describe "sharding plugin" do
  before do
    @Artist = Class.new(Sequel::Model(:artists)) do
      columns :id, :name

      def self.y
        {:id=>2, :name=>'YJM'}
      end
    end
    @Album = Class.new(Sequel::Model(:albums)) do
      columns :id, :artist_id, :name

      def self.ds_ext(m=nil)
        @ds_ext = m if m
        @ds_ext
      end

      def self.y
        {:id=>1, :name=>'RF', :artist_id=>2}
      end
      
      private

      def _join_table_dataset(opts)
        ds = super
        m = model
        ds.meta_def(:model){m}
        ds.extend model.ds_ext
        ds
      end
    end
    @Tag = Class.new(Sequel::Model(:tags)) do
      columns :id, :name

      def self.y
        {:id=>3, :name=>'M'}
      end
    end
    models = [@Artist, @Album, @Tag]
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id
    @Album.many_to_one :artist, :class=>@Artist
    @Album.many_to_many :tags, :class=>@Tag, :left_key=>:album_id, :right_key=>:tag_id, :join_table=>:albums_tags
    m = Module.new do
      def actions
        @actions ||= []
      end
    end
    models.each do |model|
      model.extend m
      model.plugin :sharding
      model.dataset.extend(ds_ext = Module.new do
        def insert(h={})
          model.actions << [:insert, h.dup, opts[:server]]
          1
        end
        def delete
          model.actions << [:delete,(literal(opts[:where]) if opts[:where]), opts[:server]]
          1
        end
        def update(h={})
          model.actions << [:update, h.dup, (literal(opts[:where]) if opts[:where]), opts[:server]]
          1
        end
        def fetch_rows(sql)
          model.actions << [:fetch, (literal(opts[:where] || opts[:join]) if opts[:where] || opts[:join]), opts[:server]]
          yield(model.y)
        end
      end)
      @Album.ds_ext(ds_ext)
    end
    @db = Sequel::Model.db
  end 

  specify "should allow you to instantiate a new object for a specified shard" do
    @Album.new_using_server(:s1, :name=>'RF').save
    @Album.actions.should == [[:insert, {:name=>"RF"}, :s1], [:fetch, "(id = 1)", :s1]]
    
    @Album.actions.clear
    @Album.new_using_server(:s2){|o| o.name = 'MO'}.save
    @Album.actions.should == [[:insert, {:name=>"MO"}, :s2], [:fetch, "(id = 1)", :s2]]
  end 

  specify "should allow you to create and save a new object for a specified shard" do
    @Album.create_using_server(:s1, :name=>'RF')
    @Album.actions.should == [[:insert, {:name=>"RF"}, :s1], [:fetch, "(id = 1)", :s1]]
    
    @Album.actions.clear
    @Album.create_using_server(:s2){|o| o.name = 'MO'}
    @Album.actions.should == [[:insert, {:name=>"MO"}, :s2], [:fetch, "(id = 1)", :s2]]
  end 

  specify "should have objects retrieved from a specific shard update that shard" do
    @Album.server(:s1).first.update(:name=>'MO')
    @Album.actions.should == [[:fetch, nil, :s1], [:update, {:name=>"MO"}, "(id = 1)", :s1]]
  end 

  specify "should have objects retrieved from a specific shard delete from that shard" do
    @Album.server(:s1).first.delete
    @Album.actions.should == [[:fetch, nil, :s1], [:delete, "(id = 1)", :s1]]
  end 

  specify "should have objects retrieved from a specific shard reload from that shard" do
    @Album.server(:s1).first.reload
    @Album.actions.should == [[:fetch, nil, :s1], [:fetch, "(id = 1)", :s1]]
  end 

  specify "should use current dataset's shard when eager loading if eagerly loaded dataset doesn't have it's own shard" do
    albums = @Album.server(:s1).eager(:artist).all
    @Album.actions.should == [[:fetch, nil, :s1]]
    @Artist.actions.should == [[:fetch, "(artists.id IN (2))", :s1]]
    @Artist.actions.clear
    albums.length == 1
    albums.first.artist.save
    @Artist.actions.should == [[:update, {:name=>"YJM"}, "(id = 2)", :s1]]
  end 

  specify "should not use current dataset's shard when eager loading if eagerly loaded dataset has its own shard" do
    @Artist.dataset.opts[:server] = :s2
    albums = @Album.server(:s1).eager(:artist).all
    @Album.actions.should == [[:fetch, nil, :s1]]
    @Artist.actions.should == [[:fetch, "(artists.id IN (2))", :s2]]
    @Artist.actions.clear
    albums.length == 1
    albums.first.artist.save
    @Artist.actions.should == [[:update, {:name=>"YJM"}, "(id = 2)", :s2]]
  end 

  specify "should have objects retrieved from a specific shard use associated objects from that shard, with modifications to the associated objects using that shard" do
    album = @Album.server(:s1).first
    @Album.actions.should == [[:fetch, nil, :s1]]
    album.artist.update(:name=>'AS')
    @Artist.actions.should == [[:fetch, "(artists.id = 2)", :s1], [:update, {:name=>"AS"}, "(id = 2)", :s1]]
    album.tags.map{|a| a.update(:name=>'SR')}
    @Tag.actions.should == [[:fetch, "( INNER JOIN albums_tags ON ((albums_tags.tag_id = tags.id) AND (albums_tags.album_id = 1)))", :s1], [:update, {:name=>"SR"}, "(id = 3)", :s1]]
    
    @Album.actions.clear
    @Artist.actions.clear
    @Artist.server(:s2).first.albums.map{|a| a.update(:name=>'MO')}
    @Artist.actions.should == [[:fetch, nil, :s2]]
    @Album.actions.should == [[:fetch, "(albums.artist_id = 2)", :s2], [:update, {:name=>"MO"}, "(id = 1)", :s2]]
  end 

  specify "should have objects retrieved from a specific shard add associated objects to that shard" do
    album = @Album.server(:s1).first
    artist = @Artist.server(:s2).first
    @Album.actions.clear
    @Artist.actions.clear

    artist.add_album(:name=>'MO')
    @Album.actions.should == [[:insert, {:name=>"MO", :artist_id=>2}, :s2], [:fetch, "(id = 1)", :s2]]
    @Album.actions.clear
    
    album.add_tag(:name=>'SR')
    @Tag.actions.should == [[:insert, {:name=>"SR"}, :s1], [:fetch, "(id = 1)", :s1]]
    @Album.actions.should == [[:insert, {:album_id=>1, :tag_id=>3}, :s1]]
  end 

  specify "should have objects retrieved from a specific shard remove associated objects from that shard" do
    album = @Album.server(:s1).first
    artist = @Artist.server(:s2).first
    @Album.actions.clear
    @Artist.actions.clear

    artist.remove_album(1)
    @Album.actions.should == [[:fetch, "((albums.artist_id = 2) AND (albums.id = 1))", :s2], [:update, {:name=>"RF", :artist_id=>nil}, "(id = 1)", :s2]]
    @Album.actions.clear
    
    album.remove_tag(3)
    @Tag.actions.should == [[:fetch, "(tags.id = 3)", :s1]]
    @Album.actions.should == [[:delete, "((album_id = 1) AND (tag_id = 3))", :s1]]
  end 

  specify "should have objects retrieved from a specific shard remove all associated objects from that shard" do
    album = @Album.server(:s1).first
    artist = @Artist.server(:s2).first
    @Album.actions.clear
    @Artist.actions.clear

    artist.remove_all_albums
    @Album.actions.should == [[:update, {:artist_id=>nil}, "(artist_id = 2)", :s2]]
    @Album.actions.clear
    
    album.remove_all_tags
    @Album.actions.should == [[:delete, "(album_id = 1)", :s1]]
  end 

  specify "should not override a server already set on an associated object" do
    album = @Album.server(:s1).first
    artist = @Artist.server(:s2).first
    @Album.actions.clear
    @Artist.actions.clear

    artist.add_album(@Album.load(:id=>4, :name=>'MO').set_server(:s3))
    @Album.actions.should == [[:update, {:name=>"MO", :artist_id=>2}, "(id = 4)", :s3]]
    @Album.actions.clear

    artist.remove_album(@Album.load(:id=>5, :name=>'T', :artist_id=>2).set_server(:s4))
    # Should select from current object's shard to check existing association, but update associated object's shard
    @Album.actions.should == [[:fetch, "((albums.artist_id = 2) AND (id = 5))", :s2], [:update, {:name=>"T", :artist_id=>nil}, "(id = 5)", :s4]]
    @Album.actions.clear
  end 

  specify "should be able to set a shard to use for any object using set_server" do
    @Album.server(:s1).first.set_server(:s2).reload
    @Album.actions.should == [[:fetch, nil, :s1], [:fetch, "(id = 1)", :s2]]
  end 
end
