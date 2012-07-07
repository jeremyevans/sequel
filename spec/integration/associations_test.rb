require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

shared_examples_for "eager limit strategies" do
  specify "eager loading one_to_one associations should work correctly" do
    Artist.one_to_one :first_album, {:clone=>:first_album}.merge(@els) if @els
    Artist.one_to_one  :last_album, {:clone=>:last_album}.merge(@els) if @els
    @album.update(:artist => @artist)
    diff_album = @diff_album.call
    al, ar, t = @pr.call

    a = Artist.eager(:first_album, :last_album).order(:name).all
    a.should == [@artist, ar]
    a.first.first_album.should == @album
    a.first.last_album.should == diff_album
    a.last.first_album.should == nil
    a.last.last_album.should == nil

    # Check that no extra columns got added by the eager loading
    a.first.first_album.values.should == @album.values
    a.first.last_album.values.should == diff_album.values

    same_album = @same_album.call
    a = Artist.eager(:first_album).order(:name).all
    a.should == [@artist, ar]
    [@album, same_album].should include(a.first.first_album)
    a.last.first_album.should == nil
  end

  specify "should correctly handle limits and offsets when eager loading one_to_many associations" do
    Artist.one_to_many :first_two_albums, {:clone=>:first_two_albums}.merge(@els) if @els
    Artist.one_to_many :second_two_albums, {:clone=>:second_two_albums}.merge(@els) if @els
    Artist.one_to_many :last_two_albums, {:clone=>:last_two_albums}.merge(@els) if @els
    @album.update(:artist => @artist)
    middle_album = @middle_album.call
    diff_album = @diff_album.call
    al, ar, t = @pr.call

    ars = Artist.eager(:first_two_albums, :second_two_albums, :last_two_albums).order(:name).all
    ars.should == [@artist, ar]
    ars.first.first_two_albums.should == [@album, middle_album]
    ars.first.second_two_albums.should == [middle_album, diff_album]
    ars.first.last_two_albums.should == [diff_album, middle_album]
    ars.last.first_two_albums.should == []
    ars.last.second_two_albums.should == []
    ars.last.last_two_albums.should == []

    # Check that no extra columns got added by the eager loading
    ars.first.first_two_albums.map{|x| x.values}.should == [@album, middle_album].map{|x| x.values}
    ars.first.second_two_albums.map{|x| x.values}.should == [middle_album, diff_album].map{|x| x.values}
    ars.first.last_two_albums.map{|x| x.values}.should == [diff_album, middle_album].map{|x| x.values}
  end

  specify "should correctly handle limits and offsets when eager loading many_to_many associations" do
    if @els == {:eager_limit_strategy=>:correlated_subquery} && Sequel.guarded?(:derby)
      pending("Derby errors with correlated subqueries on many_to_many associations")
    end
    Album.many_to_many :first_two_tags, {:clone=>:first_two_tags}.merge(@els) if @els
    Album.many_to_many :second_two_tags, {:clone=>:second_two_tags}.merge(@els) if @els
    Album.many_to_many :last_two_tags, {:clone=>:last_two_tags}.merge(@els) if @els
    tu, tv = @other_tags.call
    al, ar, t = @pr.call

    als = Album.eager(:first_two_tags, :second_two_tags, :last_two_tags).order(:name).all
    als.should == [@album, al]
    als.first.first_two_tags.should == [@tag, tu]
    als.first.second_two_tags.should == [tu, tv]
    als.first.last_two_tags.should == [tv, tu]
    als.last.first_two_tags.should == []
    als.last.second_two_tags.should == []
    als.last.last_two_tags.should == []

    # Check that no extra columns got added by the eager loading
    als.first.first_two_tags.map{|x| x.values}.should == [@tag, tu].map{|x| x.values}
    als.first.second_two_tags.map{|x| x.values}.should == [tu, tv].map{|x| x.values}
    als.first.last_two_tags.map{|x| x.values}.should == [tv, tu].map{|x| x.values}
  end

  specify "should correctly handle limits and offsets when eager loading many_through_many associations" do
    if @els == {:eager_limit_strategy=>:correlated_subquery} && Sequel.guarded?(:derby)
      pending("Derby errors with correlated subqueries on many_through_many associations")
    end
    Artist.many_through_many :first_two_tags, {:clone=>:first_two_tags}.merge(@els) if @els
    Artist.many_through_many :second_two_tags, {:clone=>:second_two_tags}.merge(@els) if @els
    Artist.many_through_many :last_two_tags, {:clone=>:last_two_tags}.merge(@els) if @els
    @album.update(:artist => @artist)
    tu, tv = @other_tags.call
    al, ar, t = @pr.call

    ars = Artist.eager(:first_two_tags, :second_two_tags, :last_two_tags).order(:name).all
    ars.should == [@artist, ar]
    ars.first.first_two_tags.should == [@tag, tu]
    ars.first.second_two_tags.should == [tu, tv]
    ars.first.last_two_tags.should == [tv, tu]
    ars.last.first_two_tags.should == []
    ars.last.second_two_tags.should == []
    ars.last.last_two_tags.should == []

    # Check that no extra columns got added by the eager loading
    ars.first.first_two_tags.map{|x| x.values}.should == [@tag, tu].map{|x| x.values}
    ars.first.second_two_tags.map{|x| x.values}.should == [tu, tv].map{|x| x.values}
    ars.first.last_two_tags.map{|x| x.values}.should == [tv, tu].map{|x| x.values}
  end
end

shared_examples_for "filtering/excluding by associations" do
  specify "should work correctly when filtering by associations" do
    @album.update(:artist => @artist)
    @album.add_tag(@tag)

    @Artist.filter(:albums=>@album).all.should == [@artist]
    @Artist.filter(:first_album=>@album).all.should == [@artist]
    @Album.filter(:artist=>@artist).all.should == [@album]
    @Album.filter(:tags=>@tag).all.should == [@album]
    @Album.filter(:alias_tags=>@tag).all.should == [@album]
    @Tag.filter(:albums=>@album).all.should == [@tag]
    @Album.filter(:artist=>@artist, :tags=>@tag).all.should == [@album]
    @artist.albums_dataset.filter(:tags=>@tag).all.should == [@album]
  end

  specify "should work correctly when excluding by associations" do
    @album.update(:artist => @artist)
    @album.add_tag(@tag)
    album, artist, tag = @pr.call

    @Artist.exclude(:albums=>@album).all.should == [artist]
    @Artist.exclude(:first_album=>@album).all.should == [artist]
    @Album.exclude(:artist=>@artist).all.should == [album]
    @Album.exclude(:tags=>@tag).all.should == [album]
    @Album.exclude(:alias_tags=>@tag).all.should == [album]
    @Tag.exclude(:albums=>@album).all.should == [tag]
    @Album.exclude(:artist=>@artist, :tags=>@tag).all.should == [album]
  end

  specify "should work correctly when filtering by multiple associations" do
    album, artist, tag = @pr.call
    @album.update(:artist => @artist)
    @album.add_tag(@tag)

    @Artist.filter(:albums=>[@album, album]).all.should == [@artist]
    @Artist.filter(:first_album=>[@album, album]).all.should == [@artist]
    @Album.filter(:artist=>[@artist, artist]).all.should == [@album]
    @Album.filter(:tags=>[@tag, tag]).all.should == [@album]
    @Album.filter(:alias_tags=>[@tag, tag]).all.should == [@album]
    @Tag.filter(:albums=>[@album, album]).all.should == [@tag]
    @Album.filter(:artist=>[@artist, artist], :tags=>[@tag, tag]).all.should == [@album]
    @artist.albums_dataset.filter(:tags=>[@tag, tag]).all.should == [@album]

    album.add_tag(tag)

    @Artist.filter(:albums=>[@album, album]).all.should == [@artist]
    @Artist.filter(:first_album=>[@album, album]).all.should == [@artist]
    @Album.filter(:artist=>[@artist, artist]).all.should == [@album]
    @Album.filter(:tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.filter(:alias_tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@album, album]
    @Tag.filter(:albums=>[@album, album]).all.sort_by{|x| x.pk}.should == [@tag, tag]
    @Album.filter(:artist=>[@artist, artist], :tags=>[@tag, tag]).all.should == [@album]

    album.update(:artist => artist)

    @Artist.filter(:albums=>[@album, album]).all.sort_by{|x| x.pk}.should == [@artist, artist]
    @Artist.filter(:first_album=>[@album, album]).all.sort_by{|x| x.pk}.should == [@artist, artist]
    @Album.filter(:artist=>[@artist, artist]).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.filter(:tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.filter(:alias_tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@album, album]
    @Tag.filter(:albums=>[@album, album]).all.sort_by{|x| x.pk}.should == [@tag, tag]
    @Album.filter(:artist=>[@artist, artist], :tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@album, album]
  end

  specify "should work correctly when excluding by multiple associations" do
    album, artist, tag = @pr.call

    @Artist.exclude(:albums=>[@album, album]).all.sort_by{|x| x.pk}.should == [@artist, artist]
    @Artist.exclude(:first_album=>[@album, album]).all.sort_by{|x| x.pk}.should == [@artist, artist]
    @Album.exclude(:artist=>[@artist, artist]).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.exclude(:tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.exclude(:alias_tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@album, album]
    @Tag.exclude(:albums=>[@album, album]).all.sort_by{|x| x.pk}.should == [@tag, tag]
    @Album.exclude(:artist=>[@artist, artist], :tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@album, album]

    @album.update(:artist => @artist)
    @album.add_tag(@tag)

    @Artist.exclude(:albums=>[@album, album]).all.sort_by{|x| x.pk}.should == [artist]
    @Artist.exclude(:first_album=>[@album, album]).all.sort_by{|x| x.pk}.should == [artist]
    @Album.exclude(:artist=>[@artist, artist]).all.sort_by{|x| x.pk}.should == [album]
    @Album.exclude(:tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [album]
    @Album.exclude(:alias_tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [album]
    @Tag.exclude(:albums=>[@album, album]).all.sort_by{|x| x.pk}.should == [tag]
    @Album.exclude(:artist=>[@artist, artist], :tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [album]

    album.add_tag(tag)

    @Artist.exclude(:albums=>[@album, album]).all.should == [artist]
    @Artist.exclude(:first_album=>[@album, album]).all.should == [artist]
    @Album.exclude(:artist=>[@artist, artist]).all.should == [album]
    @Album.exclude(:tags=>[@tag, tag]).all.should == []
    @Album.exclude(:alias_tags=>[@tag, tag]).all.should == []
    @Tag.exclude(:albums=>[@album, album]).all.should == []
    @Album.exclude(:artist=>[@artist, artist], :tags=>[@tag, tag]).all.should == [album]

    album.update(:artist => artist)

    @Artist.exclude(:albums=>[@album, album]).all.should == []
    @Artist.exclude(:first_album=>[@album, album]).all.should == []
    @Album.exclude(:artist=>[@artist, artist]).all.should == []
    @Album.exclude(:tags=>[@tag, tag]).all.should == []
    @Album.exclude(:alias_tags=>[@tag, tag]).all.should == []
    @Tag.exclude(:albums=>[@album, album]).all.should == []
    @Album.exclude(:artist=>[@artist, artist], :tags=>[@tag, tag]).all.should == []
  end

  specify "should work correctly when excluding by associations in regards to NULL values" do
    @Artist.exclude(:albums=>@album).all.should == [@artist]
    @Artist.exclude(:first_album=>@album).all.should == [@artist]
    @Album.exclude(:artist=>@artist).all.should == [@album]
    @Album.exclude(:tags=>@tag).all.should == [@album]
    @Album.exclude(:alias_tags=>@tag).all.should == [@album]
    @Tag.exclude(:albums=>@album).all.should == [@tag]
    @Album.exclude(:artist=>@artist, :tags=>@tag).all.should == [@album]

    @album.update(:artist => @artist)
    @artist.albums_dataset.exclude(:tags=>@tag).all.should == [@album]
  end

  specify "should handle NULL values in join table correctly when filtering/excluding many_to_many associations" do
    @ins.call
    @Album.exclude(:tags=>@tag).all.should == [@album]
    @Album.exclude(:alias_tags=>@tag).all.should == [@album]
    @album.add_tag(@tag)
    @Album.filter(:tags=>@tag).all.should == [@album]
    @Album.filter(:alias_tags=>@tag).all.should == [@album]
    album, artist, tag = @pr.call
    @Album.exclude(:tags=>@tag).all.should == [album]
    @Album.exclude(:alias_tags=>@tag).all.should == [album]
    @Album.exclude(:tags=>tag).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.exclude(:alias_tags=>tag).all.sort_by{|x| x.pk}.should == [@album, album]
  end

  specify "should work correctly when filtering by association datasets" do
    album, artist, tag = @pr.call
    @album.update(:artist => @artist)
    @album.add_tag(@tag)
    album.add_tag(tag)
    album.update(:artist => artist)

    @Artist.filter(:albums=>@Album).all.sort_by{|x| x.pk}.should == [@artist, artist]
    @Artist.filter(:albums=>@Album.filter(Array(Album.primary_key).map{|k| k.qualify(Album.table_name)}.zip(Array(album.pk)))).all.sort_by{|x| x.pk}.should == [artist]
    @Artist.filter(:albums=>@Album.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
    @Artist.filter(:first_album=>@Album).all.sort_by{|x| x.pk}.should == [@artist, artist]
    @Artist.filter(:first_album=>@Album.filter(Array(Album.primary_key).map{|k| k.qualify(Album.table_name)}.zip(Array(album.pk)))).all.sort_by{|x| x.pk}.should == [artist]
    @Artist.filter(:first_album=>@Album.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
    @Album.filter(:artist=>@Artist).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.filter(:artist=>@Artist.filter(Array(Artist.primary_key).map{|k| k.qualify(Artist.table_name)}.zip(Array(artist.pk)))).all.sort_by{|x| x.pk}.should == [album]
    @Album.filter(:artist=>@Artist.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
    @Album.filter(:tags=>@Tag).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.filter(:tags=>@Tag.filter(Array(Tag.primary_key).map{|k| k.qualify(Tag.table_name)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [album]
    @Album.filter(:tags=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
    @Album.filter(:alias_tags=>@Tag).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.filter(:alias_tags=>@Tag.filter(Array(Tag.primary_key).map{|k| k.qualify(Tag.table_name)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [album]
    @Album.filter(:alias_tags=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
    @Tag.filter(:albums=>@Album).all.sort_by{|x| x.pk}.should == [@tag, tag]
    @Tag.filter(:albums=>@Album.filter(Array(Album.primary_key).map{|k| k.qualify(Album.table_name)}.zip(Array(album.pk)))).all.sort_by{|x| x.pk}.should == [tag]
    @Tag.filter(:albums=>@Album.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
  end

  specify "should work correctly when excluding by association datasets" do
    album, artist, tag = @pr.call
    @album.update(:artist => @artist)
    @album.add_tag(@tag)
    album.add_tag(tag)
    album.update(:artist => artist)

    @Artist.exclude(:albums=>@Album).all.sort_by{|x| x.pk}.should == []
    @Artist.exclude(:albums=>@Album.filter(Array(Album.primary_key).map{|k| k.qualify(Album.table_name)}.zip(Array(album.pk)))).all.sort_by{|x| x.pk}.should == [@artist]
    @Artist.exclude(:albums=>@Album.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@artist, artist]
    @Album.exclude(:artist=>@Artist).all.sort_by{|x| x.pk}.should == []
    @Album.exclude(:artist=>@Artist.filter(Array(Artist.primary_key).map{|k| k.qualify(Artist.table_name)}.zip(Array(artist.pk)))).all.sort_by{|x| x.pk}.should == [@album]
    @Album.exclude(:artist=>@Artist.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.exclude(:tags=>@Tag).all.sort_by{|x| x.pk}.should == []
    @Album.exclude(:tags=>@Tag.filter(Array(Tag.primary_key).map{|k| k.qualify(Tag.table_name)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [@album]
    @Album.exclude(:tags=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.exclude(:alias_tags=>@Tag).all.sort_by{|x| x.pk}.should == []
    @Album.exclude(:alias_tags=>@Tag.filter(Array(Tag.primary_key).map{|k| k.qualify(Tag.table_name)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [@album]
    @Album.exclude(:alias_tags=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@album, album]
    @Tag.exclude(:albums=>@Album).all.sort_by{|x| x.pk}.should == []
    @Tag.exclude(:albums=>@Album.filter(Array(Album.primary_key).map{|k| k.qualify(Album.table_name)}.zip(Array(album.pk)))).all.sort_by{|x| x.pk}.should == [@tag]
    @Tag.exclude(:albums=>@Album.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@tag, tag]
  end
end

shared_examples_for "regular and composite key associations" do
  specify "should return no objects if none are associated" do
    @album.artist.should == nil
    @artist.first_album.should == nil
    @artist.albums.should == []
    @album.tags.should == []
    @album.alias_tags.should == []
    @tag.albums.should == []
  end

  specify "should have add and set methods work any associated objects" do
    @album.update(:artist => @artist)
    @album.add_tag(@tag)

    @album.reload
    @artist.reload
    @tag.reload

    @album.artist.should == @artist
    @artist.first_album.should == @album
    @artist.albums.should == [@album]
    @album.tags.should == [@tag]
    @album.alias_tags.should == [@tag]
    @tag.albums.should == [@album]
  end

  specify "should work correctly with prepared_statements_association plugin" do
    @album.update(:artist => @artist)
    @album.add_tag(@tag)

    @album.reload
    @artist.reload
    @tag.reload

    [Tag, Album, Artist].each{|x| x.plugin :prepared_statements_associations}
    @album.artist.should == @artist
    @artist.first_album.should == @album
    @artist.albums.should == [@album]
    @album.tags.should == [@tag]
    @album.alias_tags.should == [@tag]
    @tag.albums.should == [@album]
  end

  specify "should have working dataset associations" do
    album, artist, tag = @pr.call

    Tag.albums.all.should == []
    Album.artists.all.should == []
    Album.tags.all.should == []
    Album.alias_tags.all.should == []
    Artist.albums.all.should == []
    Artist.tags.all.should == []
    Artist.albums.tags.all.should == []

    @album.update(:artist => @artist)
    @album.add_tag(@tag)

    Tag.albums.all.should == [@album]
    Album.artists.all.should == [@artist]
    Album.tags.all.should == [@tag]
    Album.alias_tags.all.should == [@tag]
    Artist.albums.all.should == [@album]
    Artist.tags.all.should == [@tag]
    Artist.albums.tags.all.should == [@tag]

    album.add_tag(tag)
    album.update(:artist => artist)

    Tag.albums.order(:name).all.should == [@album, album]
    Album.artists.order(:name).all.should == [@artist, artist]
    Album.tags.order(:name).all.should == [@tag, tag]
    Album.alias_tags.order(:name).all.should == [@tag, tag]
    Artist.albums.order(:name).all.should == [@album, album]
    Artist.tags.order(:name).all.should == [@tag, tag]
    Artist.albums.tags.order(:name).all.should == [@tag, tag]

    Tag.filter(Tag.qualified_primary_key_hash(tag.pk)).albums.all.should == [album]
    Album.filter(Album.qualified_primary_key_hash(album.pk)).artists.all.should == [artist]
    Album.filter(Album.qualified_primary_key_hash(album.pk)).tags.all.should == [tag]
    Album.filter(Album.qualified_primary_key_hash(album.pk)).alias_tags.all.should == [tag]
    Artist.filter(Artist.qualified_primary_key_hash(artist.pk)).albums.all.should == [album]
    Artist.filter(Artist.qualified_primary_key_hash(artist.pk)).tags.all.should == [tag]
    Artist.filter(Artist.qualified_primary_key_hash(artist.pk)).albums.tags.all.should == [tag]

    Artist.filter(Artist.qualified_primary_key_hash(artist.pk)).albums.filter(Album.qualified_primary_key_hash(album.pk)).tags.all.should == [tag]
    Artist.filter(Artist.qualified_primary_key_hash(@artist.pk)).albums.filter(Album.qualified_primary_key_hash(@album.pk)).tags.all.should == [@tag]
    Artist.filter(Artist.qualified_primary_key_hash(@artist.pk)).albums.filter(Album.qualified_primary_key_hash(album.pk)).tags.all.should == []
    Artist.filter(Artist.qualified_primary_key_hash(artist.pk)).albums.filter(Album.qualified_primary_key_hash(@album.pk)).tags.all.should == []
  end

  specify "should have remove methods work" do
    @album.update(:artist => @artist)
    @album.add_tag(@tag)

    @album.update(:artist => nil)
    @album.remove_tag(@tag)

    @album.add_alias_tag(@tag)
    @album.remove_alias_tag(@tag)

    @album.reload
    @artist.reload
    @tag.reload

    @album.artist.should == nil
    @artist.albums.should == []
    @album.tags.should == []
    @tag.albums.should == []

    @album.add_alias_tag(@tag)
    @album.remove_alias_tag(@tag)

    @album.reload
    @album.alias_tags.should == []
  end

  specify "should have remove_all methods work" do
    @artist.add_album(@album)
    @album.add_tag(@tag)

    @artist.remove_all_albums
    @album.remove_all_tags

    @album.reload
    @artist.reload
    @tag.reload

    @album.artist.should == nil
    @artist.albums.should == []
    @album.tags.should == []
    @tag.albums.should == []

    @album.add_alias_tag(@tag)
    @album.remove_all_alias_tags

    @album.reload
    @album.alias_tags.should == []
  end

  specify "should eager load via eager correctly" do
    @album.update(:artist => @artist)
    @album.add_tag(@tag)

    a = Artist.eager(:albums=>[:tags, :alias_tags]).eager(:first_album).all
    a.should == [@artist]
    a.first.albums.should == [@album]
    a.first.first_album.should == @album
    a.first.albums.first.tags.should == [@tag]
    a.first.albums.first.alias_tags.should == [@tag]

    a = Tag.eager(:albums=>:artist).all
    a.should == [@tag]
    a.first.albums.should == [@album]
    a.first.albums.first.artist.should == @artist
  end

  describe "when filtering/excluding by associations" do
    before do
      @Artist = Artist.dataset
      @Album = Album.dataset
      @Tag = Tag.dataset
    end

    it_should_behave_like "filtering/excluding by associations"
  end

  describe "when filtering/excluding by associations when joining" do
    def self_join(c)
      c.join(c.table_name.as(:b), Array(c.primary_key).zip(Array(c.primary_key))).select_all(c.table_name)
    end

    before do
      @Artist = self_join(Artist)
      @Album = self_join(Album)
      @Tag = self_join(Tag)
    end

    it_should_behave_like "filtering/excluding by associations"
  end

  describe "with no :eager_limit_strategy" do
    it_should_behave_like "eager limit strategies"
  end

  describe "with :eager_limit_strategy=>true" do
    before do
      @els = {:eager_limit_strategy=>true}
    end
    it_should_behave_like "eager limit strategies"
  end

  describe "with :eager_limit_strategy=>:window_function" do
    before do
      @els = {:eager_limit_strategy=>:window_function}
    end
    it_should_behave_like "eager limit strategies"
  end if INTEGRATION_DB.dataset.supports_window_functions?

  specify "should eager load via eager_graph correctly" do
    @album.update(:artist => @artist)
    @album.add_tag(@tag)

    a = Artist.eager_graph(:albums=>[:tags, :alias_tags]).eager_graph(:first_album).all
    a.should == [@artist]
    a.first.albums.should == [@album]
    a.first.first_album.should == @album
    a.first.albums.first.tags.should == [@tag]
    a.first.albums.first.alias_tags.should == [@tag]

    a = Tag.eager_graph(:albums=>:artist).all
    a.should == [@tag]
    a.first.albums.should == [@album]
    a.first.albums.first.artist.should == @artist
  end

  specify "should work with a many_through_many association" do
    @album.update(:artist => @artist)
    @album.add_tag(@tag)

    @album.reload
    @artist.reload
    @tag.reload

    @album.tags.should == [@tag]

    a = Artist.eager(:tags).all
    a.should == [@artist]
    a.first.tags.should == [@tag]

    a = Artist.eager_graph(:tags).all
    a.should == [@artist]
    a.first.tags.should == [@tag]

    a = Album.eager(:artist=>:tags).all
    a.should == [@album]
    a.first.artist.should == @artist
    a.first.artist.tags.should == [@tag]

    a = Album.eager_graph(:artist=>:tags).all
    a.should == [@album]
    a.first.artist.should == @artist
    a.first.artist.tags.should == [@tag]
  end
end

describe "Sequel::Model Simple Associations" do
  before(:all) do
    @db = INTEGRATION_DB
    @db.drop_table?(:albums_tags, :tags, :albums, :artists)
    @db.create_table(:artists) do
      primary_key :id
      String :name
    end
    @db.create_table(:albums) do
      primary_key :id
      String :name
      foreign_key :artist_id, :artists
    end
    @db.create_table(:tags) do
      primary_key :id
      String :name
    end
    @db.create_table(:albums_tags) do
      foreign_key :album_id, :albums
      foreign_key :tag_id, :tags
    end
  end
  before do
    [:albums_tags, :tags, :albums, :artists].each{|t| @db[t].delete}
    class ::Artist < Sequel::Model(@db)
      plugin :dataset_associations
      one_to_many :albums, :order=>:name
      one_to_one :first_album, :class=>:Album, :order=>:name
      one_to_one :last_album, :class=>:Album, :order=>:name.desc
      one_to_many :first_two_albums, :class=>:Album, :order=>:name, :limit=>2
      one_to_many :second_two_albums, :class=>:Album, :order=>:name, :limit=>[2, 1]
      one_to_many :last_two_albums, :class=>:Album, :order=>:name.desc, :limit=>2
      plugin :many_through_many
      many_through_many :tags, [[:albums, :artist_id, :id], [:albums_tags, :album_id, :tag_id]]
      many_through_many :first_two_tags, :clone=>:tags, :order=>:tags__name, :limit=>2
      many_through_many :second_two_tags, :clone=>:tags, :order=>:tags__name, :limit=>[2, 1]
      many_through_many :last_two_tags, :clone=>:tags, :order=>:tags__name.desc, :limit=>2
    end
    class ::Album < Sequel::Model(@db)
      plugin :dataset_associations
      many_to_one :artist
      many_to_many :tags, :right_key=>:tag_id
      many_to_many :alias_tags, :clone=>:tags, :join_table=>:albums_tags___at
      many_to_many :first_two_tags, :clone=>:tags, :order=>:name, :limit=>2
      many_to_many :second_two_tags, :clone=>:tags, :order=>:name, :limit=>[2, 1]
      many_to_many :last_two_tags, :clone=>:tags, :order=>:name.desc, :limit=>2
    end
    class ::Tag < Sequel::Model(@db)
      plugin :dataset_associations
      many_to_many :albums
    end
    @album = Album.create(:name=>'Al')
    @artist = Artist.create(:name=>'Ar')
    @tag = Tag.create(:name=>'T')
    @same_album = lambda{Album.create(:name=>'Al', :artist_id=>@artist.id)}
    @diff_album = lambda{Album.create(:name=>'lA', :artist_id=>@artist.id)}
    @middle_album = lambda{Album.create(:name=>'Bl', :artist_id=>@artist.id)}
    @other_tags = lambda{t = [Tag.create(:name=>'U'), Tag.create(:name=>'V')]; @db[:albums_tags].insert([:album_id, :tag_id], Tag.select(@album.id, :id)); t}
    @pr = lambda{[Album.create(:name=>'Al2'),Artist.create(:name=>'Ar2'),Tag.create(:name=>'T2')]}
    @ins = lambda{@db[:albums_tags].insert(:tag_id=>@tag.id)}
  end
  after do
    [:Tag, :Album, :Artist].each{|x| Object.send(:remove_const, x)}
  end
  after(:all) do
    @db.drop_table?(:albums_tags, :tags, :albums, :artists)
  end

  it_should_behave_like "regular and composite key associations"

  describe "with :eager_limit_strategy=>:correlated_subquery" do
    before do
      @els = {:eager_limit_strategy=>:correlated_subquery}
    end
    it_should_behave_like "eager limit strategies"
  end unless Sequel.guarded?(:mysql, :db2, :oracle, :h2)

  specify "should handle many_to_one associations with same name as :key" do
    Album.def_column_alias(:artist_id_id, :artist_id)
    Album.many_to_one :artist_id, :key_column =>:artist_id, :class=>Artist
    @album.update(:artist_id_id => @artist.id)
    @album.artist_id.should == @artist

    as = Album.eager(:artist_id).all
    as.should == [@album]
    as.map{|a| a.artist_id}.should == [@artist]

    as = Album.eager_graph(:artist_id).all
    as.should == [@album]
    as.map{|a| a.artist_id}.should == [@artist]
  end

  specify "should handle aliased tables when eager_graphing" do
    @album.update(:artist => @artist)
    @album.add_tag(@tag)

    Artist.set_dataset(:artists___ar)
    Album.set_dataset(:albums___a)
    Tag.set_dataset(:tags___t)
    Artist.one_to_many :balbums, :class=>Album, :key=>:artist_id
    Album.many_to_many :btags, :class=>Tag, :join_table=>:albums_tags, :right_key=>:tag_id
    Album.many_to_one :bartist, :class=>Artist, :key=>:artist_id
    Tag.many_to_many :balbums, :class=>Album, :join_table=>:albums_tags, :right_key=>:album_id

    a = Artist.eager_graph(:balbums=>:btags).all
    a.should == [@artist]
    a.first.balbums.should == [@album]
    a.first.balbums.first.btags.should == [@tag]

    a = Tag.eager_graph(:balbums=>:bartist).all
    a.should == [@tag]
    a.first.balbums.should == [@album]
    a.first.balbums.first.bartist.should == @artist
  end

  specify "should have add method accept hashes and create new records" do
    @artist.remove_all_albums
    Album.delete
    @album = @artist.add_album(:name=>'Al2')
    Album.first[:name].should == 'Al2'
    @artist.albums_dataset.first[:name].should == 'Al2'

    @album.remove_all_tags
    Tag.delete
    @album.add_tag(:name=>'T2')
    Tag.first[:name].should == 'T2'
    @album.tags_dataset.first[:name].should == 'T2'
  end

  specify "should have add method accept primary key and add related records" do
    @artist.remove_all_albums
    @artist.add_album(@album.id)
    @artist.albums_dataset.first[:id].should == @album.id

    @album.remove_all_tags
    @album.add_tag(@tag.id)
    @album.tags_dataset.first[:id].should == @tag.id
  end

  specify "should have remove method accept primary key and remove related album" do
    @artist.add_album(@album)
    @artist.reload.remove_album(@album.id)
    @artist.reload.albums.should == []

    @album.add_tag(@tag)
    @album.reload.remove_tag(@tag.id)
    @tag.reload.albums.should == []
  end

  specify "should handle dynamic callbacks for regular loading" do
    @artist.add_album(@album)

    @artist.albums.should == [@album]
    @artist.albums(proc{|ds| ds.exclude(:id=>@album.id)}).should == []
    @artist.albums(proc{|ds| ds.filter(:id=>@album.id)}).should == [@album]

    @album.artist.should == @artist
    @album.artist(proc{|ds| ds.exclude(:id=>@artist.id)}).should == nil
    @album.artist(proc{|ds| ds.filter(:id=>@artist.id)}).should == @artist

    @artist.albums{|ds| ds.exclude(:id=>@album.id)}.should == []
    @artist.albums{|ds| ds.filter(:id=>@album.id)}.should == [@album]
    @album.artist{|ds| ds.exclude(:id=>@artist.id)}.should == nil
    @album.artist{|ds| ds.filter(:id=>@artist.id)}.should == @artist
  end

  specify "should handle dynamic callbacks for eager loading via eager and eager_graph" do
    @artist.add_album(@album)
    @album.add_tag(@tag)
    album2 = @artist.add_album(:name=>'Foo')
    tag2 = album2.add_tag(:name=>'T2')

    artist = Artist.eager(:albums=>:tags).all.first
    artist.albums.should == [@album, album2]
    artist.albums.map{|x| x.tags}.should == [[@tag], [tag2]]

    artist = Artist.eager_graph(:albums=>:tags).all.first
    artist.albums.should == [@album, album2]
    artist.albums.map{|x| x.tags}.should == [[@tag], [tag2]]

    artist = Artist.eager(:albums=>{proc{|ds| ds.where(:id=>album2.id)}=>:tags}).all.first
    artist.albums.should == [album2]
    artist.albums.first.tags.should == [tag2]

    artist = Artist.eager_graph(:albums=>{proc{|ds| ds.where(:id=>album2.id)}=>:tags}).all.first
    artist.albums.should == [album2]
    artist.albums.first.tags.should == [tag2]
  end

  specify "should have remove method raise an error for one_to_many records if the object isn't already associated" do
    proc{@artist.remove_album(@album.id)}.should raise_error(Sequel::Error)
    proc{@artist.remove_album(@album)}.should raise_error(Sequel::Error)
  end
end

describe "Sequel::Model Composite Key Associations" do
  before(:all) do
    @db = INTEGRATION_DB
    @db.drop_table?(:albums_tags, :tags, :albums, :artists)
    @db.create_table(:artists) do
      Integer :id1
      Integer :id2
      String :name
      primary_key [:id1, :id2]
    end
    @db.create_table(:albums) do
      Integer :id1
      Integer :id2
      String :name
      Integer :artist_id1
      Integer :artist_id2
      foreign_key [:artist_id1, :artist_id2], :artists
      primary_key [:id1, :id2]
    end
    @db.create_table(:tags) do
      Integer :id1
      Integer :id2
      String :name
      primary_key [:id1, :id2]
    end
    @db.create_table(:albums_tags) do
      Integer :album_id1
      Integer :album_id2
      Integer :tag_id1
      Integer :tag_id2
      foreign_key [:album_id1, :album_id2], :albums
      foreign_key [:tag_id1, :tag_id2], :tags
    end
  end
  before do
    [:albums_tags, :tags, :albums, :artists].each{|t| @db[t].delete}
    class ::Artist < Sequel::Model(@db)
      plugin :dataset_associations
      set_primary_key :id1, :id2
      unrestrict_primary_key
      one_to_many :albums, :key=>[:artist_id1, :artist_id2], :order=>:name
      one_to_one :first_album, :clone=>:albums, :order=>:name
      one_to_one :last_album, :clone=>:albums, :order=>:name.desc
      one_to_many :first_two_albums, :clone=>:albums, :order=>:name, :limit=>2
      one_to_many :second_two_albums, :clone=>:albums, :order=>:name, :limit=>[2, 1]
      one_to_many :last_two_albums, :clone=>:albums, :order=>:name.desc, :limit=>2
      plugin :many_through_many
      many_through_many :tags, [[:albums, [:artist_id1, :artist_id2], [:id1, :id2]], [:albums_tags, [:album_id1, :album_id2], [:tag_id1, :tag_id2]]]
      many_through_many :first_two_tags, :clone=>:tags, :order=>:tags__name, :limit=>2
      many_through_many :second_two_tags, :clone=>:tags, :order=>:tags__name, :limit=>[2, 1]
      many_through_many :last_two_tags, :clone=>:tags, :order=>:tags__name.desc, :limit=>2
    end
    class ::Album < Sequel::Model(@db)
      plugin :dataset_associations
      set_primary_key :id1, :id2
      unrestrict_primary_key
      many_to_one :artist, :key=>[:artist_id1, :artist_id2]
      many_to_many :tags, :left_key=>[:album_id1, :album_id2], :right_key=>[:tag_id1, :tag_id2]
      many_to_many :alias_tags, :clone=>:tags, :join_table=>:albums_tags___at
      many_to_many :first_two_tags, :clone=>:tags, :order=>:name, :limit=>2
      many_to_many :second_two_tags, :clone=>:tags, :order=>:name, :limit=>[2, 1]
      many_to_many :last_two_tags, :clone=>:tags, :order=>:name.desc, :limit=>2
    end
    class ::Tag < Sequel::Model(@db)
      plugin :dataset_associations
      set_primary_key :id1, :id2
      unrestrict_primary_key
      many_to_many :albums, :right_key=>[:album_id1, :album_id2], :left_key=>[:tag_id1, :tag_id2]
    end
    @album = Album.create(:name=>'Al', :id1=>1, :id2=>2)
    @artist = Artist.create(:name=>'Ar', :id1=>3, :id2=>4)
    @tag = Tag.create(:name=>'T', :id1=>5, :id2=>6)
    @same_album = lambda{Album.create(:name=>'Al', :id1=>7, :id2=>8, :artist_id1=>3, :artist_id2=>4)}
    @diff_album = lambda{Album.create(:name=>'lA', :id1=>9, :id2=>10, :artist_id1=>3, :artist_id2=>4)}
    @middle_album = lambda{Album.create(:name=>'Bl', :id1=>13, :id2=>14, :artist_id1=>3, :artist_id2=>4)}
    @other_tags = lambda{t = [Tag.create(:name=>'U', :id1=>17, :id2=>18), Tag.create(:name=>'V', :id1=>19, :id2=>20)]; @db[:albums_tags].insert([:album_id1, :album_id2, :tag_id1, :tag_id2], Tag.select(1, 2, :id1, :id2)); t}
    @pr = lambda{[Album.create(:name=>'Al2', :id1=>11, :id2=>12),Artist.create(:name=>'Ar2', :id1=>13, :id2=>14),Tag.create(:name=>'T2', :id1=>15, :id2=>16)]}
    @ins = lambda{@db[:albums_tags].insert(:tag_id1=>@tag.id1, :tag_id2=>@tag.id2)}
  end
  after do
    [:Tag, :Album, :Artist].each{|x| Object.send(:remove_const, x)}
  end
  after(:all) do
    @db.drop_table?(:albums_tags, :tags, :albums, :artists)
  end

  it_should_behave_like "regular and composite key associations"

  describe "with :eager_limit_strategy=>:correlated_subquery" do
    before do
      @els = {:eager_limit_strategy=>:correlated_subquery}
    end
    it_should_behave_like "eager limit strategies"
  end if INTEGRATION_DB.dataset.supports_multiple_column_in? && !Sequel.guarded?(:mysql, :db2, :oracle)

  specify "should have add method accept hashes and create new records" do
    @artist.remove_all_albums
    Album.delete
    @artist.add_album(:id1=>1, :id2=>2, :name=>'Al2')
    Album.first[:name].should == 'Al2'
    @artist.albums_dataset.first[:name].should == 'Al2'

    @album.remove_all_tags
    Tag.delete
    @album.add_tag(:id1=>1, :id2=>2, :name=>'T2')
    Tag.first[:name].should == 'T2'
    @album.tags_dataset.first[:name].should == 'T2'
  end

  specify "should have add method accept primary key and add related records" do
    @artist.remove_all_albums
    @artist.add_album([@album.id1, @album.id2])
    @artist.albums_dataset.first.pk.should == [@album.id1, @album.id2]

    @album.remove_all_tags
    @album.add_tag([@tag.id1, @tag.id2])
    @album.tags_dataset.first.pk.should == [@tag.id1, @tag.id2]
  end

  specify "should have remove method accept primary key and remove related album" do
    @artist.add_album(@album)
    @artist.reload.remove_album([@album.id1, @album.id2])
    @artist.reload.albums.should == []

    @album.add_tag(@tag)
    @album.reload.remove_tag([@tag.id1, @tag.id2])
    @tag.reload.albums.should == []
  end

  specify "should have remove method raise an error for one_to_many records if the object isn't already associated" do
    proc{@artist.remove_album([@album.id1, @album.id2])}.should raise_error(Sequel::Error)
    proc{@artist.remove_album(@album)}.should raise_error(Sequel::Error)
  end
end

describe "Sequel::Model Associations with clashing column names" do
  before(:all) do
    @db = INTEGRATION_DB
    @db.drop_table?(:bars_foos, :bars, :foos)
    @db.create_table(:foos) do
      primary_key :id
      Integer :object_id
    end
    @db.create_table(:bars) do
      primary_key :id
      Integer :object_id
    end
    @db.create_table(:bars_foos) do
      Integer :foo_id
      Integer :object_id
      primary_key [:foo_id, :object_id]
    end
  end
  before do
    [:bars_foos, :bars, :foos].each{|t| @db[t].delete}
    @Foo = Class.new(Sequel::Model(:foos))
    @Bar = Class.new(Sequel::Model(:bars))
    @Foo.def_column_alias(:obj_id, :object_id)
    @Bar.def_column_alias(:obj_id, :object_id)
    @Foo.one_to_many :bars, :primary_key=>:obj_id, :primary_key_column=>:object_id, :key=>:object_id, :key_method=>:obj_id, :class=>@Bar
    @Foo.one_to_one :bar, :primary_key=>:obj_id, :primary_key_column=>:object_id, :key=>:object_id, :key_method=>:obj_id, :class=>@Bar
    @Bar.many_to_one :foo, :key=>:obj_id, :key_column=>:object_id, :primary_key=>:object_id, :primary_key_method=>:obj_id, :class=>@Foo
    @Foo.many_to_many :mtmbars, :join_table=>:bars_foos, :left_primary_key=>:obj_id, :left_primary_key_column=>:object_id, :right_primary_key=>:object_id, :right_primary_key_method=>:obj_id, :left_key=>:foo_id, :right_key=>:object_id, :class=>@Bar
    @Bar.many_to_many :mtmfoos, :join_table=>:bars_foos, :left_primary_key=>:obj_id, :left_primary_key_column=>:object_id, :right_primary_key=>:object_id, :right_primary_key_method=>:obj_id, :left_key=>:object_id, :right_key=>:foo_id, :class=>@Foo
    @foo = @Foo.create(:obj_id=>2)
    @bar = @Bar.create(:obj_id=>2)
    @Foo.db[:bars_foos].insert(2, 2)
  end
  after(:all) do
    @db.drop_table?(:bars_foos, :bars, :foos)
  end

  it "should have working regular association methods" do
    @Bar.first.foo.should == @foo
    @Foo.first.bars.should == [@bar]
    @Foo.first.bar.should == @bar
    @Foo.first.mtmbars.should == [@bar]
    @Bar.first.mtmfoos.should == [@foo]
  end

  it "should have working eager loading methods" do
    @Bar.eager(:foo).all.map{|o| [o, o.foo]}.should == [[@bar, @foo]]
    @Foo.eager(:bars).all.map{|o| [o, o.bars]}.should == [[@foo, [@bar]]]
    @Foo.eager(:bar).all.map{|o| [o, o.bar]}.should == [[@foo, @bar]]
    @Foo.eager(:mtmbars).all.map{|o| [o, o.mtmbars]}.should == [[@foo, [@bar]]]
    @Bar.eager(:mtmfoos).all.map{|o| [o, o.mtmfoos]}.should == [[@bar, [@foo]]]
  end

  it "should have working eager graphing methods" do
    @Bar.eager_graph(:foo).all.map{|o| [o, o.foo]}.should == [[@bar, @foo]]
    @Foo.eager_graph(:bars).all.map{|o| [o, o.bars]}.should == [[@foo, [@bar]]]
    @Foo.eager_graph(:bar).all.map{|o| [o, o.bar]}.should == [[@foo, @bar]]
    @Foo.eager_graph(:mtmbars).all.map{|o| [o, o.mtmbars]}.should == [[@foo, [@bar]]]
    @Bar.eager_graph(:mtmfoos).all.map{|o| [o, o.mtmfoos]}.should == [[@bar, [@foo]]]
  end

  it "should have working modification methods" do
    b = @Bar.create(:obj_id=>3)
    f = @Foo.create(:obj_id=>3)

    @bar.foo = f
    @bar.obj_id.should == 3
    @foo.bar = @bar
    @bar.obj_id.should == 2

    @foo.add_bar(b)
    @foo.bars.sort_by{|x| x.obj_id}.should == [@bar, b]
    @foo.remove_bar(b)
    @foo.bars.should == [@bar]
    @foo.remove_all_bars
    @foo.bars.should == []

    @bar.refresh.update(:obj_id=>2)
    b.refresh.update(:obj_id=>3)
    @foo.mtmbars.should == [@bar]
    @foo.remove_all_mtmbars
    @foo.mtmbars.should == []
    @foo.add_mtmbar(b)
    @foo.mtmbars.should == [b]
    @foo.remove_mtmbar(b)
    @foo.mtmbars.should == []

    @bar.add_mtmfoo(f)
    @bar.mtmfoos.should == [f]
    @bar.remove_all_mtmfoos
    @bar.mtmfoos.should == []
    @bar.add_mtmfoo(f)
    @bar.mtmfoos.should == [f]
    @bar.remove_mtmfoo(f)
    @bar.mtmfoos.should == []
  end
end
