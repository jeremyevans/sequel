require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

shared_examples_for "one_to_one eager limit strategies" do
  specify "eager loading one_to_one associations should work correctly" do
    Artist.one_to_one :first_album, {:clone=>:first_album}.merge(@els) if @els
    Artist.one_to_one  :last_album, {:clone=>:last_album}.merge(@els) if @els
    Artist.one_to_one  :second_album, {:clone=>:second_album}.merge(@els) if @els && @els[:eager_limit_strategy] != :distinct_on
    @album.update(:artist => @artist)
    diff_album = @diff_album.call
    ar = @pr.call[1]
    
    a = Artist.eager(:first_album, :last_album, :second_album).order(:name).all
    a.should == [@artist, ar]
    a.first.first_album.should == @album
    a.first.last_album.should == diff_album
    a.first.second_album.should == diff_album
    a.last.first_album.should == nil
    a.last.last_album.should == nil
    a.last.second_album.should == nil

    # Check that no extra columns got added by the eager loading
    a.first.first_album.values.should == @album.values
    a.first.last_album.values.should == diff_album.values
    a.first.second_album.values.should == diff_album.values

    same_album = @same_album.call
    a = Artist.eager(:first_album).order(:name).all
    a.should == [@artist, ar]
    [@album, same_album].should include(a.first.first_album)
    a.last.first_album.should == nil
  end
end

shared_examples_for "one_to_one eager_graph limit strategies" do
  specify "eager graphing one_to_one associations should work correctly" do
    @album.update(:artist => @artist)
    diff_album = @diff_album.call
    ar = @pr.call[1]
    ds = Artist.order(:artists__name)
    limit_strategy = {:limit_strategy=>@els[:eager_limit_strategy]}
    
    a = ds.eager_graph_with_options(:first_album, limit_strategy).all
    a.should == [@artist, ar]
    a.first.first_album.should == @album
    a.last.first_album.should == nil
    a.first.first_album.values.should == @album.values

    a = ds.eager_graph_with_options(:last_album, limit_strategy).all
    a = ds.eager_graph(:last_album).all
    a.should == [@artist, ar]
    a.first.last_album.should == diff_album
    a.last.last_album.should == nil
    a.first.last_album.values.should == diff_album.values

    if @els[:eager_limit_strategy] != :distinct_on && (@els[:eager_limit_strategy] != :correlated_subquery || Album.dataset.supports_offsets_in_correlated_subqueries?) 
      a = ds.eager_graph_with_options(:second_album, limit_strategy).all
      a = ds.eager_graph(:second_album).all
      a.should == [@artist, ar]
      a.first.second_album.should == diff_album
      a.last.second_album.should == nil
      a.first.second_album.values.should == diff_album.values
    end

    same_album = @same_album.call
    a = ds.eager_graph_with_options(:first_album, limit_strategy).all
    a.should == [@artist, ar]
    [@album, same_album].should include(a.first.first_album)
    a.last.first_album.should == nil
  end
end

shared_examples_for "one_to_many eager limit strategies" do
  specify "should correctly handle limits and offsets when eager loading one_to_many associations" do
    Artist.one_to_many :first_two_albums, {:clone=>:first_two_albums}.merge(@els) if @els
    Artist.one_to_many :second_two_albums, {:clone=>:second_two_albums}.merge(@els) if @els
    Artist.one_to_many :not_first_albums, {:clone=>:not_first_albums}.merge(@els) if @els
    Artist.one_to_many :last_two_albums, {:clone=>:last_two_albums}.merge(@els) if @els
    @album.update(:artist => @artist)
    middle_album = @middle_album.call
    diff_album = @diff_album.call
    ar = @pr.call[1]
    
    ars = Artist.eager(:first_two_albums, :second_two_albums, :not_first_albums, :last_two_albums).order(:name).all
    ars.should == [@artist, ar]
    ars.first.first_two_albums.should == [@album, middle_album]
    ars.first.second_two_albums.should == [middle_album, diff_album]
    ars.first.not_first_albums.should == [middle_album, diff_album]
    ars.first.last_two_albums.should == [diff_album, middle_album]
    ars.last.first_two_albums.should == []
    ars.last.second_two_albums.should == []
    ars.last.not_first_albums.should == []
    ars.last.last_two_albums.should == []
    
    # Check that no extra columns got added by the eager loading
    ars.first.first_two_albums.map{|x| x.values}.should == [@album, middle_album].map{|x| x.values}
    ars.first.second_two_albums.map{|x| x.values}.should == [middle_album, diff_album].map{|x| x.values}
    ars.first.not_first_albums.map{|x| x.values}.should == [middle_album, diff_album].map{|x| x.values}
    ars.first.last_two_albums.map{|x| x.values}.should == [diff_album, middle_album].map{|x| x.values}
  end
end

shared_examples_for "one_to_many eager_graph limit strategies" do
  specify "should correctly handle limits and offsets when eager graphing one_to_many associations" do
    @album.update(:artist => @artist)
    middle_album = @middle_album.call
    diff_album = @diff_album.call
    ar = @pr.call[1]
    ds = Artist.order(:artists__name)
    limit_strategy = {:limit_strategy=>@els[:eager_limit_strategy]}
    
    ars = ds.eager_graph_with_options(:first_two_albums, limit_strategy).all
    ars.should == [@artist, ar]
    ars.first.first_two_albums.should == [@album, middle_album]
    ars.last.first_two_albums.should == []
    ars.first.first_two_albums.map{|x| x.values}.should == [@album, middle_album].map{|x| x.values}

    if @els[:eager_limit_strategy] != :correlated_subquery || Album.dataset.supports_offsets_in_correlated_subqueries?
      ars = ds.eager_graph_with_options(:second_two_albums, limit_strategy).all
      ars.should == [@artist, ar]
      ars.first.second_two_albums.should == [middle_album, diff_album]
      ars.last.second_two_albums.should == []
      ars.first.second_two_albums.map{|x| x.values}.should == [middle_album, diff_album].map{|x| x.values}

      ars = ds.eager_graph_with_options(:not_first_albums, limit_strategy).all
      ars.should == [@artist, ar]
      ars.first.not_first_albums.should == [middle_album, diff_album]
      ars.last.not_first_albums.should == []
      ars.first.not_first_albums.map{|x| x.values}.should == [middle_album, diff_album].map{|x| x.values}
    end

    ars = ds.eager_graph_with_options(:last_two_albums, limit_strategy).all
    ars.should == [@artist, ar]
    ars.first.last_two_albums.should == [diff_album, middle_album]
    ars.last.last_two_albums.should == []
    ars.first.last_two_albums.map{|x| x.values}.should == [diff_album, middle_album].map{|x| x.values}
  end
end

shared_examples_for "one_through_one eager limit strategies" do
  specify "should correctly handle offsets when eager loading one_through_one associations" do
    Album.one_through_one :first_tag, {:clone=>:first_tag}.merge(@els) if @els
    Album.one_through_one :second_tag, {:clone=>:second_tag}.merge(@els) if @els && @els[:eager_limit_strategy] != :distinct_on
    Album.one_through_one :last_tag, {:clone=>:last_tag}.merge(@els) if @els
    tu, tv = @other_tags.call
    al = @pr.call.first
    
    als = Album.eager(:first_tag, :second_tag, :last_tag).order(:name).all
    als.should == [@album, al]
    als.first.first_tag.should == @tag
    als.first.second_tag.should == tu
    als.first.last_tag.should == tv
    als.last.first_tag.should == nil
    als.last.second_tag.should == nil
    als.last.last_tag.should == nil
    
    # Check that no extra columns got added by the eager loading
    als.first.first_tag.values.should == @tag.values
    als.first.second_tag.values.should == tu.values
    als.first.last_tag.values.should == tv.values
  end
end

shared_examples_for "one_through_one eager_graph limit strategies" do
  specify "should correctly handle offsets when eager graphing one_through_one associations" do
    tu, tv = @other_tags.call
    al = @pr.call.first
    ds = Album.order(:albums__name)
    limit_strategy = {:limit_strategy=>@els[:eager_limit_strategy]}
    
    als = ds.eager_graph_with_options(:first_tag, limit_strategy).all
    als.should == [@album, al]
    als.first.first_tag.should == @tag
    als.last.first_tag.should == nil
    als.first.first_tag.values.should == @tag.values

    als = ds.eager_graph_with_options(:second_tag, @els[:eager_limit_strategy] != :distinct_on ? limit_strategy : {}).all
    als.should == [@album, al]
    als.first.second_tag.should == tu
    als.last.second_tag.should == nil
    als.first.second_tag.values.should == tu.values

    als = ds.eager_graph_with_options(:last_tag, limit_strategy).all
    als.should == [@album, al]
    als.first.last_tag.should == tv
    als.last.last_tag.should == nil
    als.first.last_tag.values.should == tv.values
  end
end

shared_examples_for "many_to_many eager limit strategies" do
  specify "should correctly handle limits and offsets when eager loading many_to_many associations" do
    Album.send @many_to_many_method||:many_to_many, :first_two_tags, {:clone=>:first_two_tags}.merge(@els) if @els
    Album.send @many_to_many_method||:many_to_many, :second_two_tags, {:clone=>:second_two_tags}.merge(@els) if @els
    Album.send @many_to_many_method||:many_to_many, :not_first_tags, {:clone=>:not_first_tags}.merge(@els) if @els
    Album.send @many_to_many_method||:many_to_many, :last_two_tags, {:clone=>:last_two_tags}.merge(@els) if @els
    tu, tv = @other_tags.call
    al = @pr.call.first
    al.add_tag(tu)
    
    als = Album.eager(:first_two_tags, :second_two_tags, :not_first_tags, :last_two_tags).order(:name).all
    als.should == [@album, al]
    als.first.first_two_tags.should == [@tag, tu]
    als.first.second_two_tags.should == [tu, tv]
    als.first.not_first_tags.should == [tu, tv]
    als.first.last_two_tags.should == [tv, tu]
    als.last.first_two_tags.should == [tu]
    als.last.second_two_tags.should == []
    als.last.last_two_tags.should == [tu]
    
    # Check that no extra columns got added by the eager loading
    als.first.first_two_tags.map{|x| x.values}.should == [@tag, tu].map{|x| x.values}
    als.first.second_two_tags.map{|x| x.values}.should == [tu, tv].map{|x| x.values}
    als.first.not_first_tags.map{|x| x.values}.should == [tu, tv].map{|x| x.values}
    als.first.last_two_tags.map{|x| x.values}.should == [tv, tu].map{|x| x.values}
  end
end

shared_examples_for "many_to_many eager_graph limit strategies" do
  specify "should correctly handle limits and offsets when eager loading many_to_many associations" do
    tu, tv = @other_tags.call
    al = @pr.call.first
    al.add_tag(tu)
    ds = Album.order(:albums__name)
    limit_strategy = {:limit_strategy=>(@els||{})[:eager_limit_strategy]}
    
    als = ds.eager_graph_with_options(:first_two_tags, limit_strategy).all
    als.should == [@album, al]
    als.first.first_two_tags.should == [@tag, tu]
    als.last.first_two_tags.should == [tu]
    als.first.first_two_tags.map{|x| x.values}.should == [@tag, tu].map{|x| x.values}

    als = ds.eager_graph_with_options(:second_two_tags, limit_strategy).all
    als.should == [@album, al]
    als.first.second_two_tags.should == [tu, tv]
    als.last.second_two_tags.should == []
    als.first.second_two_tags.map{|x| x.values}.should == [tu, tv].map{|x| x.values}

    als = ds.eager_graph_with_options(:not_first_tags, limit_strategy).all
    als.should == [@album, al]
    als.first.not_first_tags.should == [tu, tv]
    als.last.not_first_tags.should == []
    als.first.not_first_tags.map{|x| x.values}.should == [tu, tv].map{|x| x.values}

    als = ds.eager_graph_with_options(:last_two_tags, limit_strategy).all
    als.should == [@album, al]
    als.first.last_two_tags.should == [tv, tu]
    als.last.last_two_tags.should == [tu]
    als.first.last_two_tags.map{|x| x.values}.should == [tv, tu].map{|x| x.values}
  end
end

shared_examples_for "many_through_many eager limit strategies" do
  specify "should correctly handle limits and offsets when eager loading many_through_many associations" do
    Artist.many_through_many :first_two_tags, {:clone=>:first_two_tags}.merge(@els) if @els
    Artist.many_through_many :second_two_tags, {:clone=>:second_two_tags}.merge(@els) if @els
    Artist.many_through_many :not_first_tags, {:clone=>:not_first_tags}.merge(@els) if @els
    Artist.many_through_many :last_two_tags, {:clone=>:last_two_tags}.merge(@els) if @els
    @album.update(:artist => @artist)
    tu, tv = @other_tags.call
    al, ar, _ = @pr.call
    al.update(:artist=>ar)
    al.add_tag(tu)
    
    ars = Artist.eager(:first_two_tags, :second_two_tags, :not_first_tags, :last_two_tags).order(:name).all
    ars.should == [@artist, ar]
    ars.first.first_two_tags.should == [@tag, tu]
    ars.first.second_two_tags.should == [tu, tv]
    ars.first.not_first_tags.should == [tu, tv]
    ars.first.last_two_tags.should == [tv, tu]
    ars.last.first_two_tags.should == [tu]
    ars.last.second_two_tags.should == []
    ars.last.not_first_tags.should == []
    ars.last.last_two_tags.should == [tu]
    
    # Check that no extra columns got added by the eager loading
    ars.first.first_two_tags.map{|x| x.values}.should == [@tag, tu].map{|x| x.values}
    ars.first.second_two_tags.map{|x| x.values}.should == [tu, tv].map{|x| x.values}
    ars.first.not_first_tags.map{|x| x.values}.should == [tu, tv].map{|x| x.values}
    ars.first.last_two_tags.map{|x| x.values}.should == [tv, tu].map{|x| x.values}
  end
end

shared_examples_for "many_through_many eager_graph limit strategies" do
  specify "should correctly handle limits and offsets when eager loading many_through_many associations" do
    @album.update(:artist => @artist)
    tu, tv = @other_tags.call
    al, ar, _ = @pr.call
    al.update(:artist=>ar)
    al.add_tag(tu)
    ds = Artist.order(:artists__name)
    limit_strategy = {:limit_strategy=>@els[:eager_limit_strategy]}
    
    ars = ds.eager_graph_with_options(:first_two_tags, limit_strategy).all
    ars.should == [@artist, ar]
    ars.first.first_two_tags.should == [@tag, tu]
    ars.last.first_two_tags.should == [tu]
    ars.first.first_two_tags.map{|x| x.values}.should == [@tag, tu].map{|x| x.values}

    ars = ds.eager_graph_with_options(:second_two_tags, limit_strategy).all
    ars.should == [@artist, ar]
    ars.first.second_two_tags.should == [tu, tv]
    ars.last.second_two_tags.should == []
    ars.first.second_two_tags.map{|x| x.values}.should == [tu, tv].map{|x| x.values}

    ars = ds.eager_graph_with_options(:not_first_tags, limit_strategy).all
    ars.should == [@artist, ar]
    ars.first.not_first_tags.should == [tu, tv]
    ars.last.not_first_tags.should == []
    ars.first.not_first_tags.map{|x| x.values}.should == [tu, tv].map{|x| x.values}

    ars = ds.eager_graph_with_options(:last_two_tags, limit_strategy).all
    ars.should == [@artist, ar]
    ars.first.last_two_tags.should == [tv, tu]
    ars.last.last_two_tags.should == [tu]
    ars.first.last_two_tags.map{|x| x.values}.should == [tv, tu].map{|x| x.values}
  end
end

shared_examples_for "one_through_many eager limit strategies" do
  specify "should correctly handle offsets when eager loading one_through_many associations" do
    Artist.one_through_many :first_tag, {:clone=>:first_tag}.merge(@els) if @els
    Artist.one_through_many :second_tag, {:clone=>:second_tag}.merge(@els) if @els && @els[:eager_limit_strategy] != :distinct_on
    Artist.one_through_many :last_tag, {:clone=>:last_tag}.merge(@els) if @els
    @album.update(:artist => @artist)
    tu, tv = @other_tags.call
    al, ar, _ = @pr.call
    al.update(:artist=>ar)
    al.add_tag(tu)
    
    ars = Artist.eager(:first_tag, :second_tag, :last_tag).order(:name).all
    ars.should == [@artist, ar]
    ars.first.first_tag.should == @tag
    ars.first.second_tag.should == tu
    ars.first.last_tag.should == tv
    ars.last.first_tag.should == tu
    ars.last.second_tag.should == nil
    ars.last.last_tag.should == tu
    
    # Check that no extra columns got added by the eager loading
    ars.first.first_tag.values.should == @tag.values
    ars.first.second_tag.values.should == tu.values
    ars.first.last_tag.values.should == tv.values
  end
end

shared_examples_for "one_through_many eager_graph limit strategies" do
  specify "should correctly handle offsets when eager graphing one_through_many associations" do
    @album.update(:artist => @artist)
    tu, tv = @other_tags.call
    al, ar, _ = @pr.call
    al.update(:artist=>ar)
    al.add_tag(tu)
    ds = Artist.order(:artists__name)
    limit_strategy = {:limit_strategy=>@els[:eager_limit_strategy]}
    
    ars = ds.eager_graph_with_options(:first_tag, limit_strategy).all
    ars.should == [@artist, ar]
    ars.first.first_tag.should == @tag
    ars.last.first_tag.should == tu
    ars.first.first_tag.values.should == @tag.values

    ars = ds.eager_graph_with_options(:second_tag, @els[:eager_limit_strategy] != :distinct_on ? limit_strategy : {}).all
    ars.should == [@artist, ar]
    ars.first.second_tag.should == tu
    ars.last.second_tag.should == nil
    ars.first.second_tag.values.should == tu.values

    ars = ds.eager_graph_with_options(:last_tag, limit_strategy).all
    ars.should == [@artist, ar]
    ars.first.last_tag.should == tv
    ars.last.last_tag.should == tu
    ars.first.last_tag.values.should == tv.values
  end
end

shared_examples_for "eager limit strategies" do
  it_should_behave_like "one_to_one eager limit strategies"
  it_should_behave_like "one_to_many eager limit strategies"
  it_should_behave_like "many_to_many eager limit strategies"
  it_should_behave_like "one_through_one eager limit strategies"
  it_should_behave_like "many_through_many eager limit strategies"
  it_should_behave_like "one_through_many eager limit strategies"
end

shared_examples_for "eager_graph limit strategies" do
  it_should_behave_like "one_to_one eager_graph limit strategies"
  it_should_behave_like "one_to_many eager_graph limit strategies"
  it_should_behave_like "many_to_many eager_graph limit strategies"
  it_should_behave_like "one_through_one eager_graph limit strategies"
  it_should_behave_like "many_through_many eager_graph limit strategies"
  it_should_behave_like "one_through_many eager_graph limit strategies"
end

shared_examples_for "filtering/excluding by associations" do
  specify "should handle association inner joins" do
    @Artist.association_join(:albums).all.should == []
    @Artist.association_join(:first_album).all.should == []
    @Album.association_join(:artist).all.should == []
    @Album.association_join(:tags).all.should == []
    @Album.association_join(:alias_tags).all.should == []
    @Tag.association_join(:albums).all.should == []
    unless @no_many_through_many
      @Artist.association_join(:tags).all.should == []
      @Artist.association_join(:first_tag).all.should == []
    end

    @album.update(:artist => @artist)
    @album.add_tag(@tag)
    
    @Artist.association_join(:albums).select_all(:artists).all.should == [@artist]
    @Artist.association_join(:first_album).select_all(:artists).all.should == [@artist]
    @Album.association_join(:artist).select_all(:albums).all.should == [@album]
    @Album.association_join(:tags).select_all(:albums).all.should == [@album]
    @Album.association_join(:alias_tags).select_all(:albums).all.should == [@album]
    @Tag.association_join(:albums).select_all(:tags).all.should == [@tag]
    unless @no_many_through_many
      @Artist.association_join(:tags).select_all(:artists).all.should == [@artist]
      @Artist.association_join(:first_tag).select_all(:artists).all.should == [@artist]
    end

    @Artist.association_join(:albums).select_all(:albums).naked.all.should == [@album.values]
    @Artist.association_join(:first_album).select_all(:first_album).naked.all.should == [@album.values]
    @Album.association_join(:artist).select_all(:artist).naked.all.should == [@artist.values]
    @Album.association_join(:tags).select_all(:tags).naked.all.should == [@tag.values]
    @Album.association_join(:alias_tags).select_all(:alias_tags).naked.all.should == [@tag.values]
    @Tag.association_join(:albums).select_all(:albums).naked.all.should == [@album.values]
    unless @no_many_through_many
      @Artist.association_join(:tags).select_all(:tags).naked.all.should == [@tag.values]
      @Artist.association_join(:first_tag).select_all(:first_tag).naked.all.should == [@tag.values]
    end
  end

  specify "should handle association left joins" do
    @Artist.association_left_join(:albums).select_all(:artists).all.should == [@artist]
    @Artist.association_left_join(:first_album).select_all(:artists).all.should == [@artist]
    @Album.association_left_join(:artist).select_all(:albums).all.should == [@album]
    @Album.association_left_join(:tags).select_all(:albums).all.should == [@album]
    @Album.association_left_join(:alias_tags).select_all(:albums).all.should == [@album]
    @Tag.association_left_join(:albums).select_all(:tags).all.should == [@tag]
    unless @no_many_through_many
      @Artist.association_left_join(:tags).select_all(:artists).all.should == [@artist]
      @Artist.association_left_join(:first_tag).select_all(:artists).all.should == [@artist]
    end

    nil_hash = lambda{|obj| [obj.values.keys.inject({}){|h,k| h[k] = nil; h}]}
    @Artist.association_left_join(:albums).select_all(:albums).naked.all.should == nil_hash[@album]
    @Artist.association_left_join(:first_album).select_all(:first_album).naked.all.should == nil_hash[@album]
    @Album.association_left_join(:artist).select_all(:artist).naked.all.should == nil_hash[@artist]
    @Album.association_left_join(:tags).select_all(:tags).naked.all.should == nil_hash[@tag]
    @Album.association_left_join(:alias_tags).select_all(:alias_tags).naked.all.should == nil_hash[@tag]
    @Tag.association_left_join(:albums).select_all(:albums).naked.all.should == nil_hash[@album]
    unless @no_many_through_many
      @Artist.association_left_join(:tags).select_all(:tags).naked.all.should == nil_hash[@tag]
      @Artist.association_left_join(:first_tag).select_all(:first_tag).naked.all.should == nil_hash[@tag]
    end

    @album.update(:artist => @artist)
    @album.add_tag(@tag)
    

    @Artist.association_left_join(:albums).select_all(:albums).naked.all.should == [@album.values]
    @Artist.association_left_join(:first_album).select_all(:first_album).naked.all.should == [@album.values]
    @Album.association_left_join(:artist).select_all(:artist).naked.all.should == [@artist.values]
    @Album.association_left_join(:tags).select_all(:tags).naked.all.should == [@tag.values]
    @Album.association_left_join(:alias_tags).select_all(:alias_tags).naked.all.should == [@tag.values]
    @Tag.association_left_join(:albums).select_all(:albums).naked.all.should == [@album.values]
    unless @no_many_through_many
      @Artist.association_left_join(:tags).select_all(:tags).naked.all.should == [@tag.values]
      @Artist.association_left_join(:first_tag).select_all(:first_tag).naked.all.should == [@tag.values]
    end
  end

  specify "should work correctly when filtering by associations" do
    @album.update(:artist => @artist)
    @album.add_tag(@tag)
    
    @Artist.filter(:albums=>@album).all.should == [@artist]
    @Artist.filter(:first_album=>@album).all.should == [@artist]
    unless @no_many_through_many
      @Artist.filter(:tags=>@tag).all.should == [@artist]
      @Artist.filter(:first_tag=>@tag).all.should == [@artist]
    end
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
    unless @no_many_through_many
      @Artist.exclude(:tags=>@tag).all.should == [artist]
      @Artist.exclude(:first_tag=>@tag).all.should == [artist]
    end
    @Album.exclude(:artist=>@artist).all.should == [album]
    @Album.exclude(:tags=>@tag).all.should == [album]
    @Album.exclude(:alias_tags=>@tag).all.should == [album]
    @Tag.exclude(:albums=>@album).all.should == [tag]
    @Album.exclude(:artist=>@artist, :tags=>@tag).all.should == [album]
  end

  specify "should work correctly when filtering by associations with conditions" do
    @album.update(:artist => @artist)
    @album.add_tag(@tag)
    
    @Artist.filter(:a_albums=>@album).all.should == [@artist]
    @Artist.filter(:first_a_album=>@album).all.should == [@artist]
    @album.update(:name=>'Foo')
    @Artist.filter(:a_albums=>@album).all.should == []
    @Artist.filter(:first_a_album=>@album).all.should == []

    @Album.filter(:a_artist=>@artist).all.should == [@album]
    @artist.update(:name=>'Foo')
    @Album.filter(:a_artist=>@artist).all.should == []

    @Album.filter(:t_tags=>@tag).all.should == [@album]
    @Album.filter(:alias_t_tags=>@tag).all.should == [@album]
    unless @no_many_through_many
      @Album.filter(:t_tag=>@tag).all.should == [@album]
      @Album.filter(:alias_t_tag=>@tag).all.should == [@album]
      @Artist.filter(:t_tags=>@tag).all.should == [@artist]
      @Artist.filter(:t_tag=>@tag).all.should == [@artist]
    end
    @tag.update(:name=>'Foo')
    @Album.filter(:t_tags=>@tag).all.should == []
    @Album.filter(:alias_t_tags=>@tag).all.should == []
    unless @no_many_through_many
      @Album.filter(:t_tag=>@tag).all.should == []
      @Album.filter(:alias_t_tag=>@tag).all.should == []
      @Artist.filter(:t_tags=>@tag).all.should == []
      @Artist.filter(:t_tag=>@tag).all.should == []
    end
  end
  
  specify "should work correctly when excluding by associations with conditions" do
    @album.update(:artist => @artist)
    @album.add_tag(@tag)
    
    @Artist.exclude(:a_albums=>@album).all.should == []
    @Artist.exclude(:first_a_album=>@album).all.should == []
    @album.update(:name=>'Foo')
    @Artist.exclude(:a_albums=>@album).all.should == [@artist]
    @Artist.exclude(:first_a_album=>@album).all.should == [@artist]

    @Album.exclude(:a_artist=>@artist).all.should == []
    @artist.update(:name=>'Foo')
    @Album.exclude(:a_artist=>@artist).all.should == [@album]

    @Album.exclude(:t_tags=>@tag).all.should == []
    @Album.exclude(:alias_t_tags=>@tag).all.should == []
    unless @no_many_through_many
      @Album.exclude(:t_tag=>@tag).all.should == []
      @Album.exclude(:alias_t_tag=>@tag).all.should == []
      @Artist.exclude(:t_tags=>@tag).all.should == []
      @Artist.exclude(:t_tag=>@tag).all.should == []
    end
    @tag.update(:name=>'Foo')
    @Album.exclude(:t_tags=>@tag).all.should == [@album]
    @Album.exclude(:alias_t_tags=>@tag).all.should == [@album]
    unless @no_many_through_many
      @Album.exclude(:t_tag=>@tag).all.should == [@album]
      @Album.exclude(:alias_t_tag=>@tag).all.should == [@album]
      @Artist.exclude(:t_tags=>@tag).all.should == [@artist]
      @Artist.exclude(:t_tag=>@tag).all.should == [@artist]
    end
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
    unless @no_many_through_many
      @Artist.filter(:tags=>[@tag, tag]).all.should == [@artist]
      @Artist.filter(:first_tag=>[@tag, tag]).all.should == [@artist]
    end

    album.add_tag(tag)

    @Artist.filter(:albums=>[@album, album]).all.should == [@artist]
    @Artist.filter(:first_album=>[@album, album]).all.should == [@artist]
    @Album.filter(:artist=>[@artist, artist]).all.should == [@album]
    @Album.filter(:tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.filter(:alias_tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@album, album]
    @Tag.filter(:albums=>[@album, album]).all.sort_by{|x| x.pk}.should == [@tag, tag]
    @Album.filter(:artist=>[@artist, artist], :tags=>[@tag, tag]).all.should == [@album]
    unless @no_many_through_many
      @Artist.filter(:tags=>[@tag, tag]).all.should == [@artist]
      @Artist.filter(:first_tag=>[@tag, tag]).all.should == [@artist]
    end

    album.update(:artist => artist)

    @Artist.filter(:albums=>[@album, album]).all.sort_by{|x| x.pk}.should == [@artist, artist]
    @Artist.filter(:first_album=>[@album, album]).all.sort_by{|x| x.pk}.should == [@artist, artist]
    @Album.filter(:artist=>[@artist, artist]).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.filter(:tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.filter(:alias_tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@album, album]
    @Tag.filter(:albums=>[@album, album]).all.sort_by{|x| x.pk}.should == [@tag, tag]
    @Album.filter(:artist=>[@artist, artist], :tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@album, album]
    unless @no_many_through_many
      @Artist.filter(:tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@artist, artist]
      @Artist.filter(:first_tag=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@artist, artist]
    end
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
    unless @no_many_through_many
      @Artist.exclude(:tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@artist, artist]
      @Artist.exclude(:first_tag=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@artist, artist]
    end

    @album.update(:artist => @artist)
    @album.add_tag(@tag)

    @Artist.exclude(:albums=>[@album, album]).all.sort_by{|x| x.pk}.should == [artist]
    @Artist.exclude(:first_album=>[@album, album]).all.sort_by{|x| x.pk}.should == [artist]
    @Album.exclude(:artist=>[@artist, artist]).all.sort_by{|x| x.pk}.should == [album]
    @Album.exclude(:tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [album]
    @Album.exclude(:alias_tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [album]
    @Tag.exclude(:albums=>[@album, album]).all.sort_by{|x| x.pk}.should == [tag]
    @Album.exclude(:artist=>[@artist, artist], :tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [album]
    unless @no_many_through_many
      @Artist.exclude(:tags=>[@tag, tag]).all.should == [artist]
      @Artist.exclude(:first_tag=>[@tag, tag]).all.should == [artist]
    end

    album.add_tag(tag)

    @Artist.exclude(:albums=>[@album, album]).all.should == [artist]
    @Artist.exclude(:first_album=>[@album, album]).all.should == [artist]
    @Album.exclude(:artist=>[@artist, artist]).all.should == [album]
    @Album.exclude(:tags=>[@tag, tag]).all.should == []
    @Album.exclude(:alias_tags=>[@tag, tag]).all.should == []
    @Tag.exclude(:albums=>[@album, album]).all.should == []
    @Album.exclude(:artist=>[@artist, artist], :tags=>[@tag, tag]).all.should == [album]
    unless @no_many_through_many
      @Artist.exclude(:tags=>[@tag, tag]).all.should == [artist]
      @Artist.exclude(:first_tag=>[@tag, tag]).all.should == [artist]
    end

    album.update(:artist => artist)

    @Artist.exclude(:albums=>[@album, album]).all.should == []
    @Artist.exclude(:first_album=>[@album, album]).all.should == []
    @Album.exclude(:artist=>[@artist, artist]).all.should == []
    @Album.exclude(:tags=>[@tag, tag]).all.should == []
    @Album.exclude(:alias_tags=>[@tag, tag]).all.should == []
    @Tag.exclude(:albums=>[@album, album]).all.should == []
    @Album.exclude(:artist=>[@artist, artist], :tags=>[@tag, tag]).all.should == []
    unless @no_many_through_many
      @Artist.exclude(:tags=>[@tag, tag]).all.should == []
      @Artist.exclude(:first_tag=>[@tag, tag]).all.should == []
    end
  end
  
  specify "should work correctly when filtering associations with conditions with multiple objects" do
    album, artist, tag = @pr.call
    album.update(:name=>@album.name)
    artist.update(:name=>@artist.name)
    tag.update(:name=>@tag.name)

    @album.update(:artist => @artist)
    @album.add_tag(@tag)
    album.update(:artist => @artist)
    tag.add_album(@album)
    
    @Artist.filter(:a_albums=>[@album, album]).all.should == [@artist]
    @Artist.filter(:first_a_album=>[@album, album]).all.should == [@artist]
    @album.update(:name=>'Foo')
    @Artist.filter(:a_albums=>[@album, album]).all.should == [@artist]
    @Artist.filter(:first_a_album=>[@album, album]).all.should == [@artist]
    album.update(:name=>'Foo')
    @Artist.filter(:a_albums=>[@album, album]).all.should == []
    @Artist.filter(:first_a_album=>[@album, album]).all.should == []

    album.update(:artist => nil)
    artist.add_album(@album)
    @Album.filter(:a_artist=>[@artist, artist]).all.should == [@album]
    @artist.update(:name=>'Foo')
    @Album.filter(:a_artist=>[@artist, artist]).all.should == [@album]
    artist.update(:name=>'Foo')
    @Album.filter(:a_artist=>[@artist, artist]).all.should == []

    @Album.filter(:t_tags=>[@tag, tag]).all.should == [@album]
    @Album.filter(:alias_t_tags=>[@tag, tag]).all.should == [@album]
    unless @no_many_through_many
      @Album.filter(:t_tag=>[@tag, tag]).all.should == [@album]
      @Album.filter(:alias_t_tag=>[@tag, tag]).all.should == [@album]
      @Artist.filter(:t_tags=>[@tag, tag]).all.should == [artist]
      @Artist.filter(:t_tag=>[@tag, tag]).all.should == [artist]
    end
    @tag.update(:name=>'Foo')
    @Album.filter(:t_tags=>[@tag, tag]).all.should == [@album]
    @Album.filter(:alias_t_tags=>[@tag, tag]).all.should == [@album]
    unless @no_many_through_many
      @Album.filter(:t_tag=>[@tag, tag]).all.should == [@album]
      @Album.filter(:alias_t_tag=>[@tag, tag]).all.should == [@album]
      @Artist.filter(:t_tags=>[@tag, tag]).all.should == [artist]
      @Artist.filter(:t_tag=>[@tag, tag]).all.should == [artist]
    end
    tag.update(:name=>'Foo')
    @Album.filter(:t_tags=>[@tag, tag]).all.should == []
    @Album.filter(:alias_t_tags=>[@tag, tag]).all.should == []
    unless @no_many_through_many
      @Album.filter(:t_tag=>[@tag, tag]).all.should == []
      @Album.filter(:alias_t_tag=>[@tag, tag]).all.should == []
      @Artist.filter(:t_tags=>[@tag, tag]).all.should == []
      @Artist.filter(:t_tag=>[@tag, tag]).all.should == []
    end
  end
  
  specify "should work correctly when excluding associations with conditions with multiple objects" do
    album, artist, tag = @pr.call
    album.update(:name=>@album.name)
    artist.update(:name=>@artist.name)
    tag.update(:name=>@tag.name)

    @album.update(:artist => @artist)
    @album.add_tag(@tag)
    album.update(:artist => @artist)
    tag.add_album(@album)
    
    artist.add_album(@album)
    @Artist.exclude(:a_albums=>[@album, album]).all.should == []
    @Artist.exclude(:first_a_album=>[@album, album]).all.should == []
    @album.update(:name=>'Foo')
    @Artist.exclude(:a_albums=>[@album, album]).all.should == [artist]
    @Artist.exclude(:first_a_album=>[@album, album]).all.should == [artist]
    album.update(:name=>'Foo')
    @Artist.exclude(:a_albums=>[@album, album]).all.sort_by{|x| x.pk}.should == [@artist, artist]
    @Artist.exclude(:first_a_album=>[@album, album]).all.sort_by{|x| x.pk}.should == [@artist, artist]

    @Album.exclude(:a_artist=>[@artist, artist]).all.should == []
    album.update(:artist => nil)
    @artist.update(:name=>'Foo')
    @Album.exclude(:a_artist=>[@artist, artist]).all.should == [album]
    artist.update(:name=>'Foo')
    @Album.exclude(:a_artist=>[@artist, artist]).all.sort_by{|x| x.pk}.should == [@album, album]

    @tag.add_album(album)
    @Album.exclude(:t_tags=>[@tag, tag]).all.should == []
    @Album.exclude(:alias_t_tags=>[@tag, tag]).all.should == []
    unless @no_many_through_many
      @Album.exclude(:t_tag=>[@tag, tag]).all.should == []
      @Album.exclude(:alias_t_tag=>[@tag, tag]).all.should == []
      @Artist.exclude(:t_tags=>[@tag, tag]).all.should == [@artist]
      @Artist.exclude(:t_tag=>[@tag, tag]).all.should == [@artist]
    end
    @tag.update(:name=>'Foo')
    @Album.exclude(:t_tags=>[@tag, tag]).all.should == [album]
    @Album.exclude(:alias_t_tags=>[@tag, tag]).all.should == [album]
    unless @no_many_through_many
      @Album.exclude(:t_tag=>[@tag, tag]).all.should == [album]
      @Album.exclude(:alias_t_tag=>[@tag, tag]).all.should == [album]
      @Artist.exclude(:t_tags=>[@tag, tag]).all.should == [@artist]
      @Artist.exclude(:t_tag=>[@tag, tag]).all.should == [@artist]
    end
    tag.update(:name=>'Foo')
    @Album.exclude(:t_tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.exclude(:alias_t_tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@album, album]
    unless @no_many_through_many
      @Album.exclude(:t_tag=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@album, album]
      @Album.exclude(:alias_t_tag=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@album, album]
      @Artist.exclude(:t_tags=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@artist, artist]
      @Artist.exclude(:t_tag=>[@tag, tag]).all.sort_by{|x| x.pk}.should == [@artist, artist]
    end
  end
  
  specify "should work correctly when excluding by associations in regards to NULL values" do
    @Artist.exclude(:albums=>@album).all.should == [@artist]
    @Artist.exclude(:first_album=>@album).all.should == [@artist]
    @Album.exclude(:artist=>@artist).all.should == [@album]
    @Album.exclude(:tags=>@tag).all.should == [@album]
    @Album.exclude(:alias_tags=>@tag).all.should == [@album]
    @Tag.exclude(:albums=>@album).all.should == [@tag]
    @Album.exclude(:artist=>@artist, :tags=>@tag).all.should == [@album]

    @Artist.exclude(:a_albums=>@album).all.should == [@artist]
    @Artist.exclude(:first_a_album=>@album).all.should == [@artist]
    @Album.exclude(:a_artist=>@artist).all.should == [@album]
    @Album.exclude(:t_tags=>@tag).all.should == [@album]
    @Album.exclude(:alias_t_tags=>@tag).all.should == [@album]
    unless @no_many_through_many
      @Album.exclude(:t_tag=>@tag).all.should == [@album]
      @Album.exclude(:alias_t_tag=>@tag).all.should == [@album]
      @Artist.exclude(:t_tags=>@tag).all.should == [@artist]
      @Artist.exclude(:t_tag=>@tag).all.should == [@artist]
    end

    @album.update(:artist => @artist)
    @artist.albums_dataset.exclude(:tags=>@tag).all.should == [@album]
  end

  specify "should handle NULL values in join table correctly when filtering/excluding many_to_many associations" do
    @ins.call
    @Album.exclude(:tags=>@tag).all.should == [@album]
    @Album.exclude(:alias_tags=>@tag).all.should == [@album]
    @Album.exclude(:t_tags=>@tag).all.should == [@album]
    @Album.exclude(:alias_t_tags=>@tag).all.should == [@album]
    @album.add_tag(@tag)
    @Album.filter(:tags=>@tag).all.should == [@album]
    @Album.filter(:alias_tags=>@tag).all.should == [@album]
    @Album.filter(:t_tags=>@tag).all.should == [@album]
    @Album.filter(:alias_t_tags=>@tag).all.should == [@album]
    album, tag = @pr.call.values_at(0, 2)
    @Album.exclude(:tags=>@tag).all.should == [album]
    @Album.exclude(:alias_tags=>@tag).all.should == [album]
    @Album.exclude(:t_tags=>@tag).all.should == [album]
    @Album.exclude(:alias_t_tags=>@tag).all.should == [album]
    @Album.exclude(:tags=>tag).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.exclude(:alias_tags=>tag).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.exclude(:t_tags=>tag).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.exclude(:alias_t_tags=>tag).all.sort_by{|x| x.pk}.should == [@album, album]
  end

  specify "should work correctly when filtering by association datasets" do
    album, artist, tag = @pr.call
    @album.update(:artist => @artist)
    @album.add_tag(@tag)
    album.add_tag(tag)
    album.update(:artist => artist)

    @Artist.filter(:albums=>@Album).all.sort_by{|x| x.pk}.should == [@artist, artist]
    @Artist.filter(:albums=>@Album.filter(Array(Album.primary_key).map{|k| Sequel.qualify(Album.table_name, k)}.zip(Array(album.pk)))).all.sort_by{|x| x.pk}.should == [artist]
    @Artist.filter(:albums=>@Album.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
    @Artist.filter(:first_album=>@Album).all.sort_by{|x| x.pk}.should == [@artist, artist]
    @Artist.filter(:first_album=>@Album.filter(Array(Album.primary_key).map{|k| Sequel.qualify(Album.table_name, k)}.zip(Array(album.pk)))).all.sort_by{|x| x.pk}.should == [artist]
    @Artist.filter(:first_album=>@Album.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
    @Album.filter(:artist=>@Artist).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.filter(:artist=>@Artist.filter(Array(Artist.primary_key).map{|k| Sequel.qualify(Artist.table_name, k)}.zip(Array(artist.pk)))).all.sort_by{|x| x.pk}.should == [album]
    @Album.filter(:artist=>@Artist.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
    @Album.filter(:tags=>@Tag).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.filter(:tags=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [album]
    @Album.filter(:tags=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
    @Album.filter(:alias_tags=>@Tag).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.filter(:alias_tags=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [album]
    @Album.filter(:alias_tags=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
    @Tag.filter(:albums=>@Album).all.sort_by{|x| x.pk}.should == [@tag, tag]
    @Tag.filter(:albums=>@Album.filter(Array(Album.primary_key).map{|k| Sequel.qualify(Album.table_name, k)}.zip(Array(album.pk)))).all.sort_by{|x| x.pk}.should == [tag]
    @Tag.filter(:albums=>@Album.filter(1=>0)).all.sort_by{|x| x.pk}.should == []

    unless @no_many_through_many
      @Artist.filter(:tags=>@Tag).all.sort_by{|x| x.pk}.should == [@artist, artist]
      @Artist.filter(:tags=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [artist]
      @Artist.filter(:tags=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
      @Artist.filter(:first_tag=>@Tag).all.sort_by{|x| x.pk}.should == [@artist, artist]
      @Artist.filter(:first_tag=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [artist]
      @Artist.filter(:first_tag=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
    end
  end

  specify "should work correctly when excluding by association datasets" do
    album, artist, tag = @pr.call
    @album.update(:artist => @artist)
    @album.add_tag(@tag)
    album.add_tag(tag)
    album.update(:artist => artist)

    @Artist.exclude(:albums=>@Album).all.sort_by{|x| x.pk}.should == []
    @Artist.exclude(:albums=>@Album.filter(Array(Album.primary_key).map{|k| Sequel.qualify(Album.table_name, k)}.zip(Array(album.pk)))).all.sort_by{|x| x.pk}.should == [@artist]
    @Artist.exclude(:albums=>@Album.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@artist, artist]
    @Artist.exclude(:first_album=>@Album).all.sort_by{|x| x.pk}.should == []
    @Artist.exclude(:first_album=>@Album.filter(Array(Album.primary_key).map{|k| Sequel.qualify(Album.table_name, k)}.zip(Array(album.pk)))).all.sort_by{|x| x.pk}.should == [@artist]
    @Artist.exclude(:first_album=>@Album.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@artist, artist]
    @Album.exclude(:artist=>@Artist).all.sort_by{|x| x.pk}.should == []
    @Album.exclude(:artist=>@Artist.filter(Array(Artist.primary_key).map{|k| Sequel.qualify(Artist.table_name, k)}.zip(Array(artist.pk)))).all.sort_by{|x| x.pk}.should == [@album]
    @Album.exclude(:artist=>@Artist.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.exclude(:tags=>@Tag).all.sort_by{|x| x.pk}.should == []
    @Album.exclude(:tags=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [@album]
    @Album.exclude(:tags=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.exclude(:alias_tags=>@Tag).all.sort_by{|x| x.pk}.should == []
    @Album.exclude(:alias_tags=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [@album]
    @Album.exclude(:alias_tags=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@album, album]
    @Tag.exclude(:albums=>@Album).all.sort_by{|x| x.pk}.should == []
    @Tag.exclude(:albums=>@Album.filter(Array(Album.primary_key).map{|k| Sequel.qualify(Album.table_name, k)}.zip(Array(album.pk)))).all.sort_by{|x| x.pk}.should == [@tag]
    @Tag.exclude(:albums=>@Album.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@tag, tag]

    unless @no_many_through_many
      @Artist.exclude(:tags=>@Tag).all.sort_by{|x| x.pk}.should == []
      @Artist.exclude(:tags=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [@artist]
      @Artist.exclude(:tags=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@artist, artist]
      @Artist.exclude(:first_tag=>@Tag).all.sort_by{|x| x.pk}.should == []
      @Artist.exclude(:first_tag=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [@artist]
      @Artist.exclude(:first_tag=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@artist, artist]
    end
  end

  specify "should work correctly when filtering by association datasets with conditions" do
    album, artist, tag = @pr.call
    @album.update(:artist => @artist)
    @album.add_tag(@tag)
    album.add_tag(tag)
    album.update(:artist => artist)

    @Artist.filter(:a_albums=>@Album).all.sort_by{|x| x.pk}.should == [@artist]
    @Artist.filter(:first_a_album=>@Album).all.sort_by{|x| x.pk}.should == [@artist]
    @Album.filter(:a_artist=>@Artist).all.sort_by{|x| x.pk}.should == [@album]
    @Album.filter(:t_tags=>@Tag).all.sort_by{|x| x.pk}.should == [@album]
    @Album.filter(:alias_t_tags=>@Tag).all.sort_by{|x| x.pk}.should == [@album]
    unless @no_many_through_many
      @Album.filter(:t_tag=>@Tag).all.sort_by{|x| x.pk}.should == [@album]
      @Album.filter(:alias_t_tag=>@Tag).all.sort_by{|x| x.pk}.should == [@album]
      @Artist.filter(:t_tags=>@Tag).all.sort_by{|x| x.pk}.should == [@artist]
      @Artist.filter(:t_tag=>@Tag).all.sort_by{|x| x.pk}.should == [@artist]
    end

    artist.update(:name=>@artist.name)
    album.update(:name=>@album.name)
    tag.update(:name=>@tag.name)

    @Artist.filter(:a_albums=>@Album).all.sort_by{|x| x.pk}.should == [@artist, artist]
    @Artist.filter(:first_a_album=>@Album).all.sort_by{|x| x.pk}.should == [@artist, artist]
    @Album.filter(:a_artist=>@Artist).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.filter(:t_tags=>@Tag).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.filter(:alias_t_tags=>@Tag).all.sort_by{|x| x.pk}.should == [@album, album]
    unless @no_many_through_many
      @Album.filter(:t_tag=>@Tag).all.sort_by{|x| x.pk}.should == [@album, album]
      @Album.filter(:alias_t_tag=>@Tag).all.sort_by{|x| x.pk}.should == [@album, album]
      @Artist.filter(:t_tags=>@Tag).all.sort_by{|x| x.pk}.should == [@artist, artist]
      @Artist.filter(:t_tag=>@Tag).all.sort_by{|x| x.pk}.should == [@artist, artist]
    end

    @Artist.filter(:a_albums=>@Album.filter(Array(Album.primary_key).map{|k| Sequel.qualify(Album.table_name, k)}.zip(Array(album.pk)))).all.sort_by{|x| x.pk}.should == [artist]
    @Artist.filter(:first_a_album=>@Album.filter(Array(Album.primary_key).map{|k| Sequel.qualify(Album.table_name, k)}.zip(Array(album.pk)))).all.sort_by{|x| x.pk}.should == [artist]
    @Album.filter(:a_artist=>@Artist.filter(Array(Artist.primary_key).map{|k| Sequel.qualify(Artist.table_name, k)}.zip(Array(artist.pk)))).all.sort_by{|x| x.pk}.should == [album]
    @Album.filter(:t_tags=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [album]
    @Album.filter(:alias_t_tags=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [album]
    unless @no_many_through_many
      @Album.filter(:t_tag=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [album]
      @Album.filter(:alias_t_tag=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [album]
      @Artist.filter(:t_tags=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [artist]
      @Artist.filter(:t_tag=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [artist]
    end

    @Artist.filter(:a_albums=>@Album.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
    @Artist.filter(:first_a_album=>@Album.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
    @Album.filter(:a_artist=>@Artist.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
    @Album.filter(:t_tags=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [album]
    @Album.filter(:t_tags=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
    @Album.filter(:alias_t_tags=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
    unless @no_many_through_many
      @Album.filter(:t_tag=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [album]
      @Album.filter(:t_tag=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
      @Album.filter(:alias_t_tag=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
      @Artist.filter(:t_tags=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
      @Artist.filter(:t_tag=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == []
    end
  end

  specify "should work correctly when excluding by association datasets with conditions" do
    album, artist, tag = @pr.call
    @album.update(:artist => @artist)
    @album.add_tag(@tag)
    album.add_tag(tag)
    album.update(:artist => artist)

    @Artist.exclude(:a_albums=>@Album).all.sort_by{|x| x.pk}.should == [artist]
    @Artist.exclude(:first_a_album=>@Album).all.sort_by{|x| x.pk}.should == [artist]
    @Album.exclude(:a_artist=>@Artist).all.sort_by{|x| x.pk}.should == [album]
    @Album.exclude(:t_tags=>@Tag).all.sort_by{|x| x.pk}.should == [album]
    @Album.exclude(:alias_t_tags=>@Tag).all.sort_by{|x| x.pk}.should == [album]
    unless @no_many_through_many
      @Album.exclude(:t_tag=>@Tag).all.sort_by{|x| x.pk}.should == [album]
      @Album.exclude(:alias_t_tag=>@Tag).all.sort_by{|x| x.pk}.should == [album]
      @Artist.exclude(:t_tags=>@Tag).all.sort_by{|x| x.pk}.should == [artist]
      @Artist.exclude(:t_tag=>@Tag).all.sort_by{|x| x.pk}.should == [artist]
    end

    artist.update(:name=>@artist.name)
    album.update(:name=>@album.name)
    tag.update(:name=>@tag.name)

    @Artist.exclude(:a_albums=>@Album).all.sort_by{|x| x.pk}.should == []
    @Artist.exclude(:first_a_album=>@Album).all.sort_by{|x| x.pk}.should == []
    @Album.exclude(:a_artist=>@Artist).all.sort_by{|x| x.pk}.should == []
    @Album.exclude(:t_tags=>@Tag).all.sort_by{|x| x.pk}.should == []
    @Album.exclude(:alias_t_tags=>@Tag).all.sort_by{|x| x.pk}.should == []
    unless @no_many_through_many
      @Album.exclude(:t_tag=>@Tag).all.sort_by{|x| x.pk}.should == []
      @Album.exclude(:alias_t_tag=>@Tag).all.sort_by{|x| x.pk}.should == []
      @Artist.exclude(:t_tags=>@Tag).all.sort_by{|x| x.pk}.should == []
      @Artist.exclude(:t_tag=>@Tag).all.sort_by{|x| x.pk}.should == []
    end

    @Artist.exclude(:a_albums=>@Album.filter(Array(Album.primary_key).map{|k| Sequel.qualify(Album.table_name, k)}.zip(Array(album.pk)))).all.sort_by{|x| x.pk}.should == [@artist]
    @Artist.exclude(:first_a_album=>@Album.filter(Array(Album.primary_key).map{|k| Sequel.qualify(Album.table_name, k)}.zip(Array(album.pk)))).all.sort_by{|x| x.pk}.should == [@artist]
    @Album.exclude(:a_artist=>@Artist.filter(Array(Artist.primary_key).map{|k| Sequel.qualify(Artist.table_name, k)}.zip(Array(artist.pk)))).all.sort_by{|x| x.pk}.should == [@album]
    @Album.exclude(:t_tags=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [@album]
    @Album.exclude(:alias_t_tags=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [@album]
    unless @no_many_through_many
      @Album.exclude(:t_tag=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [@album]
      @Album.exclude(:alias_t_tag=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [@album]
      @Artist.exclude(:t_tags=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [@artist]
      @Artist.exclude(:t_tag=>@Tag.filter(Array(Tag.primary_key).map{|k| Sequel.qualify(Tag.table_name, k)}.zip(Array(tag.pk)))).all.sort_by{|x| x.pk}.should == [@artist]
    end

    @Artist.exclude(:a_albums=>@Album.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@artist, artist]
    @Artist.exclude(:first_a_album=>@Album.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@artist, artist]
    @Album.exclude(:a_artist=>@Artist.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.exclude(:t_tags=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@album, album]
    @Album.exclude(:alias_t_tags=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@album, album]
    unless @no_many_through_many
      @Album.exclude(:t_tag=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@album, album]
      @Album.exclude(:alias_t_tag=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@album, album]
      @Artist.exclude(:t_tags=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@artist, artist]
      @Artist.exclude(:t_tag=>@Tag.filter(1=>0)).all.sort_by{|x| x.pk}.should == [@artist, artist]
    end
  end
end

shared_examples_for "filter by associations one_to_one limit strategies" do
  specify "filter by associations with limited one_to_one associations should work correctly" do
    Artist.one_to_one :first_album, {:clone=>:first_album}.merge(@els)
    Artist.one_to_one :last_album, {:clone=>:last_album}.merge(@els)
    Artist.one_to_one :second_album, {:clone=>:second_album}.merge(@els)
    @album.update(:artist => @artist)
    diff_album = @diff_album.call
    ar = @pr.call[1]
    ds = Artist.order(:name)
    
    ds.where(:first_album=>@album).all.should == [@artist]
    ds.where(:first_album=>diff_album).all.should == []
    ds.exclude(:first_album=>@album).all.should == [ar]
    ds.exclude(:first_album=>diff_album).all.should == [@artist, ar]

    if @els[:eager_limit_strategy] != :distinct_on && (@els[:eager_limit_strategy] != :correlated_subquery || Album.dataset.supports_offsets_in_correlated_subqueries?) 
      ds.where(:second_album=>@album).all.should == []
      ds.where(:second_album=>diff_album).all.should == [@artist]
      ds.exclude(:second_album=>@album).all.should == [@artist, ar]
      ds.exclude(:second_album=>diff_album).all.should == [ar]
    end

    ds.where(:last_album=>@album).all.should == []
    ds.where(:last_album=>diff_album).all.should == [@artist]
    ds.exclude(:last_album=>@album).all.should == [@artist, ar]
    ds.exclude(:last_album=>diff_album).all.should == [ar]

    Artist.one_to_one :first_album, :clone=>:first_album do |ads| ads.where(:albums__name=>diff_album.name) end
    ar.add_album(diff_album)
    ds.where(:first_album=>[@album, diff_album]).all.should == [ar]
    ds.exclude(:first_album=>[@album, diff_album]).all.should == [@artist]
  end
end

shared_examples_for "filter by associations singular association limit strategies" do
  it_should_behave_like "filter by associations one_to_one limit strategies"

  specify "dataset associations with limited one_to_one associations should work correctly" do
    Artist.one_to_one :first_album, {:clone=>:first_album}.merge(@els)
    Artist.one_to_one :last_album, {:clone=>:last_album}.merge(@els)
    Artist.one_to_one :second_album, {:clone=>:second_album}.merge(@els) if @els[:eager_limit_strategy] != :distinct_on
    @album.update(:artist => @artist)
    diff_album = @diff_album.call
    ar = @pr.call[1]
    ds = Artist
    
    ds.where(@artist.pk_hash).first_albums.all.should == [@album]
    ds.where(@artist.pk_hash).second_albums.all.should == [diff_album]
    ds.where(@artist.pk_hash).last_albums.all.should == [diff_album]
    ds.where(ar.pk_hash).first_albums.all.should == []
    ds.where(ar.pk_hash).second_albums.all.should == []
    ds.where(ar.pk_hash).last_albums.all.should == []

    Artist.one_to_one :first_album, :clone=>:first_album do |ads| ads.where(:albums__name=>diff_album.name) end
    ar.add_album(diff_album)
    ds.where(@artist.pk_hash).first_albums.all.should == []
    ds.where(ar.pk_hash).first_albums.all.should == [diff_album]
  end

  specify "filter by associations with limited one_through_one associations should work correctly" do
    Album.one_through_one :first_tag, {:clone=>:first_tag}.merge(@els)
    Album.one_through_one :second_tag, {:clone=>:second_tag}.merge(@els) if @els[:eager_limit_strategy] != :distinct_on
    Album.one_through_one :last_tag, {:clone=>:last_tag}.merge(@els)
    tu, tv = @other_tags.call
    al = @pr.call.first
    ds = Album.order(:name)
    al.add_tag(tu)
    
    ds.where(:first_tag=>@tag).all.should == [@album]
    ds.where(:first_tag=>tu).all.should == [al]
    ds.where(:first_tag=>tv).all.should == []
    ds.exclude(:first_tag=>@tag).all.should == [al]
    ds.exclude(:first_tag=>tu).all.should == [@album]
    ds.exclude(:first_tag=>tv).all.should == [@album, al]

    ds.where(:second_tag=>@tag).all.should == []
    ds.where(:second_tag=>tu).all.should == [@album]
    ds.where(:second_tag=>tv).all.should == []
    ds.exclude(:second_tag=>@tag).all.should == [@album, al]
    ds.exclude(:second_tag=>tu).all.should == [al]
    ds.exclude(:second_tag=>tv).all.should == [@album, al]

    ds.where(:last_tag=>@tag).all.should == []
    ds.where(:last_tag=>tu).all.should == [al]
    ds.where(:last_tag=>tv).all.should == [@album]
    ds.exclude(:last_tag=>@tag).all.should == [@album, al]
    ds.exclude(:last_tag=>tu).all.should == [@album]
    ds.exclude(:last_tag=>tv).all.should == [al]

    Album.one_through_one :first_tag, :clone=>:first_tag do |ads| ads.where(:tags__name=>tu.name) end
    Album.one_through_one :second_tag, :clone=>:second_tag do |ads| ads.where(:tags__name=>[tu.name, tv.name]) end

    ds.where(:first_tag=>[@tag, tu]).all.should == [@album, al]
    ds.exclude(:first_tag=>[@tag, tu]).all.should == []

    al.add_tag(tv)
    ds.where(:second_tag=>[tv, tu]).all.should == [@album, al]
    ds.exclude(:second_tag=>[tv, tu]).all.should == []
  end

  specify "dataset associations with limited one_through_one associations should work correctly" do
    Album.one_through_one :first_tag, {:clone=>:first_tag}.merge(@els)
    Album.one_through_one :second_tag, {:clone=>:second_tag}.merge(@els) if @els[:eager_limit_strategy] != :distinct_on
    Album.one_through_one :last_tag, {:clone=>:last_tag}.merge(@els)
    tu, tv = @other_tags.call
    al = @pr.call.first
    ds = Album
    al.add_tag(tu)
    
    ds.where(@album.pk_hash).first_tags.all.should == [@tag]
    ds.where(@album.pk_hash).second_tags.all.should == [tu]
    ds.where(@album.pk_hash).last_tags.all.should == [tv]
    ds.where(al.pk_hash).first_tags.all.should == [tu]
    ds.where(al.pk_hash).second_tags.all.should == []
    ds.where(al.pk_hash).last_tags.all.should == [tu]

    Album.one_through_one :first_tag, :clone=>:first_tag do |ads| ads.where(:tags__name=>tu.name) end
    Album.one_through_one :second_tag, :clone=>:second_tag do |ads| ads.where(:tags__name=>[tu.name, tv.name]) end

    ds.where(@album.pk_hash).first_tags.all.should == [tu]
    ds.where(@album.pk_hash).second_tags.all.should == [tv]
    ds.where(al.pk_hash).first_tags.all.should == [tu]
    ds.where(al.pk_hash).second_tags.all.should == []

    al.add_tag(tv)
    ds.where(@album.pk_hash).first_tags.all.should == [tu]
    ds.where(@album.pk_hash).second_tags.all.should == [tv]
    ds.where(al.pk_hash).first_tags.all.should == [tu]
    ds.where(al.pk_hash).second_tags.all.should == [tv]
  end

  specify "filter by associations with limited one_through_many associations should work correctly" do
    Artist.one_through_many :first_tag, {:clone=>:first_tag}.merge(@els)
    Artist.one_through_many :second_tag, {:clone=>:second_tag}.merge(@els) if @els[:eager_limit_strategy] != :distinct_on
    Artist.one_through_many :last_tag, {:clone=>:last_tag}.merge(@els)
    @album.update(:artist => @artist)
    tu, tv = @other_tags.call
    al, ar, _ = @pr.call
    al.update(:artist=>ar)
    al.add_tag(tu)
    ds = Artist.order(:name)

    ds.where(:first_tag=>@tag).all.should == [@artist]
    ds.where(:first_tag=>tu).all.should == [ar]
    ds.where(:first_tag=>tv).all.should == []
    ds.exclude(:first_tag=>@tag).all.should == [ar]
    ds.exclude(:first_tag=>tu).all.should == [@artist]
    ds.exclude(:first_tag=>tv).all.should == [@artist, ar]

    ds.where(:second_tag=>@tag).all.should == []
    ds.where(:second_tag=>tu).all.should == [@artist]
    ds.where(:second_tag=>tv).all.should == []
    ds.exclude(:second_tag=>@tag).all.should == [@artist, ar]
    ds.exclude(:second_tag=>tu).all.should == [ar]
    ds.exclude(:second_tag=>tv).all.should == [@artist, ar]

    ds.where(:last_tag=>@tag).all.should == []
    ds.where(:last_tag=>tu).all.should == [ar]
    ds.where(:last_tag=>tv).all.should == [@artist]
    ds.exclude(:last_tag=>@tag).all.should == [@artist, ar]
    ds.exclude(:last_tag=>tu).all.should == [@artist]
    ds.exclude(:last_tag=>tv).all.should == [ar]

    Artist.one_through_many :first_tag, :clone=>:first_tag do |ads| ads.where(:tags__name=>tu.name) end
    Artist.one_through_many :second_tag, :clone=>:second_tag do |ads| ads.where(:tags__name=>[tu.name, tv.name]) end

    ds.where(:first_tag=>[@tag, tu]).all.should == [@artist, ar]
    ds.exclude(:first_tag=>[@tag, tu]).all.should == []

    al.add_tag(tv)
    ds.where(:second_tag=>[tv, tu]).all.should == [@artist, ar]
    ds.exclude(:second_tag=>[tv, tu]).all.should == []
  end

  specify "dataset associations with limited one_through_many associations should work correctly" do
    Artist.one_through_many :first_tag, {:clone=>:first_tag}.merge(@els)
    Artist.one_through_many :second_tag, {:clone=>:second_tag}.merge(@els) if @els[:eager_limit_strategy] != :distinct_on
    Artist.one_through_many :last_tag, {:clone=>:last_tag}.merge(@els)
    @album.update(:artist => @artist)
    tu, tv = @other_tags.call
    al, ar, _ = @pr.call
    al.update(:artist=>ar)
    al.add_tag(tu)
    ds = Artist.order(:name)

    ds.where(@artist.pk_hash).first_tags.all.should == [@tag]
    ds.where(@artist.pk_hash).second_tags.all.should == [tu]
    ds.where(@artist.pk_hash).last_tags.all.should == [tv]
    ds.where(ar.pk_hash).first_tags.all.should == [tu]
    ds.where(ar.pk_hash).second_tags.all.should == []
    ds.where(ar.pk_hash).last_tags.all.should == [tu]

    Artist.one_through_many :first_tag, :clone=>:first_tag do |ads| ads.where(:tags__name=>tu.name) end
    Artist.one_through_many :second_tag, :clone=>:second_tag do |ads| ads.where(:tags__name=>[tu.name, tv.name]) end

    ds.where(@artist.pk_hash).first_tags.all.should == [tu]
    ds.where(@artist.pk_hash).second_tags.all.should == [tv]
    ds.where(ar.pk_hash).first_tags.all.should == [tu]
    ds.where(ar.pk_hash).second_tags.all.should == []

    al.add_tag(tv)
    ds.where(@artist.pk_hash).first_tags.all.should == [tu]
    ds.where(@artist.pk_hash).second_tags.all.should == [tv]
    ds.where(ar.pk_hash).first_tags.all.should == [tu]
    ds.where(ar.pk_hash).second_tags.all.should == [tv]
  end
end

shared_examples_for "filter by associations one_to_many limit strategies" do
  specify "filter by associations with limited one_to_many associations should work correctly" do
    Artist.one_to_many :first_two_albums, {:clone=>:first_two_albums}.merge(@els)
    Artist.one_to_many :second_two_albums, {:clone=>:second_two_albums}.merge(@els)
    Artist.one_to_many :not_first_albums, {:clone=>:not_first_albums}.merge(@els)
    Artist.one_to_many :last_two_albums, {:clone=>:last_two_albums}.merge(@els)
    @album.update(:artist => @artist)
    middle_album = @middle_album.call
    diff_album = @diff_album.call
    ar = @pr.call[1]
    ds = Artist.order(:name)

    ds.where(:first_two_albums=>@album).all.should == [@artist]
    ds.where(:first_two_albums=>middle_album).all.should == [@artist]
    ds.where(:first_two_albums=>diff_album).all.should == []
    ds.exclude(:first_two_albums=>@album).all.should == [ar]
    ds.exclude(:first_two_albums=>middle_album).all.should == [ar]
    ds.exclude(:first_two_albums=>diff_album).all.should == [@artist, ar]
    
    assocs = if @els[:eager_limit_strategy] != :correlated_subquery || Album.dataset.supports_offsets_in_correlated_subqueries?
      [:second_two_albums, :not_first_albums, :last_two_albums]
    else
      [:last_two_albums]
    end

    assocs.each do |a|
      ds.where(a=>@album).all.should == []
      ds.where(a=>middle_album).all.should == [@artist]
      ds.where(a=>diff_album).all.should == [@artist]
      ds.exclude(a=>@album).all.should == [@artist, ar]
      ds.exclude(a=>middle_album).all.should == [ar]
      ds.exclude(a=>diff_album).all.should == [ar]
    end

    Artist.one_to_one :first_two_albums, :clone=>:first_two_albums do |ads| ads.where(:albums__name=>diff_album.name) end
    ar.add_album(diff_album)
    ds.where(:first_two_albums=>[@album, diff_album]).all.should == [ar]
    ds.exclude(:first_two_albums=>[@album, diff_album]).all.should == [@artist]
  end
end

shared_examples_for "filter by associations limit strategies" do
  it_should_behave_like "filter by associations singular association limit strategies"
  it_should_behave_like "filter by associations one_to_many limit strategies"

  specify "dataset associations with limited one_to_many associations should work correctly" do
    Artist.one_to_many :first_two_albums, {:clone=>:first_two_albums}.merge(@els)
    Artist.one_to_many :second_two_albums, {:clone=>:second_two_albums}.merge(@els)
    Artist.one_to_many :not_first_albums, {:clone=>:not_first_albums}.merge(@els)
    Artist.one_to_many :last_two_albums, {:clone=>:last_two_albums}.merge(@els)
    @album.update(:artist => @artist)
    middle_album = @middle_album.call
    diff_album = @diff_album.call
    ar = @pr.call[1]
    ds = Artist.order(:name)

    ds.where(@artist.pk_hash).first_two_albums.all.should == [@album, middle_album]
    ds.where(@artist.pk_hash).second_two_albums.all.should == [middle_album, diff_album]
    ds.where(@artist.pk_hash).not_first_albums.all.should == [middle_album, diff_album]
    ds.where(@artist.pk_hash).last_two_albums.all.should == [diff_album, middle_album]
    ds.where(ar.pk_hash).first_two_albums.all.should == []
    ds.where(ar.pk_hash).second_two_albums.all.should == []
    ds.where(ar.pk_hash).not_first_albums.all.should == []
    ds.where(ar.pk_hash).last_two_albums.all.should == []

    Artist.one_to_one :first_two_albums, :clone=>:first_two_albums do |ads| ads.where(:albums__name=>[diff_album.name, middle_album.name]) end
    ar.add_album(diff_album)
    ds.where(@artist.pk_hash).first_two_albums.all.should == [middle_album]
    ds.where(ar.pk_hash).first_two_albums.all.should == [diff_album]
  end

  specify "filter by associations with limited many_to_many associations should work correctly" do
    Album.send :many_to_many, :first_two_tags, {:clone=>:first_two_tags}.merge(@els)
    Album.send :many_to_many, :second_two_tags, {:clone=>:second_two_tags}.merge(@els)
    Album.send :many_to_many, :not_first_tags, {:clone=>:not_first_tags}.merge(@els)
    Album.send :many_to_many, :last_two_tags, {:clone=>:last_two_tags}.merge(@els)
    tu, tv = @other_tags.call
    al = @pr.call.first
    al.add_tag(tu)
    ds = Album.order(:name)
    
    ds.where(:first_two_tags=>@tag).all.should == [@album]
    ds.where(:first_two_tags=>tu).all.should == [@album, al]
    ds.where(:first_two_tags=>tv).all.should == []
    ds.exclude(:first_two_tags=>@tag).all.should == [al]
    ds.exclude(:first_two_tags=>tu).all.should == []
    ds.exclude(:first_two_tags=>tv).all.should == [@album, al]

    ds.where(:second_two_tags=>@tag).all.should == []
    ds.where(:second_two_tags=>tu).all.should == [@album]
    ds.where(:second_two_tags=>tv).all.should == [@album]
    ds.exclude(:second_two_tags=>@tag).all.should == [@album, al]
    ds.exclude(:second_two_tags=>tu).all.should == [al]
    ds.exclude(:second_two_tags=>tv).all.should == [al]

    ds.where(:not_first_tags=>@tag).all.should == []
    ds.where(:not_first_tags=>tu).all.should == [@album]
    ds.where(:not_first_tags=>tv).all.should == [@album]
    ds.exclude(:not_first_tags=>@tag).all.should == [@album, al]
    ds.exclude(:not_first_tags=>tu).all.should == [al]
    ds.exclude(:not_first_tags=>tv).all.should == [al]

    ds.where(:last_two_tags=>@tag).all.should == []
    ds.where(:last_two_tags=>tu).all.should == [@album, al]
    ds.where(:last_two_tags=>tv).all.should == [@album]
    ds.exclude(:last_two_tags=>@tag).all.should == [@album, al]
    ds.exclude(:last_two_tags=>tu).all.should == []
    ds.exclude(:last_two_tags=>tv).all.should == [al]

    Album.many_to_many :first_two_tags, :clone=>:first_two_tags do |ads| ads.where(:tags__name=>tu.name) end
    Album.many_to_many :second_two_tags, :clone=>:second_two_tags do |ads| ads.where(:tags__name=>[tu.name, tv.name]) end

    ds.where(:first_two_tags=>[@tag, tu]).all.should == [@album, al]
    ds.exclude(:first_two_tags=>[@tag, tu]).all.should == []

    al.add_tag(tv)
    ds.where(:second_two_tags=>[tv, tu]).all.should == [@album, al]
    ds.exclude(:second_two_tags=>[tv, tu]).all.should == []
  end

  specify "dataset associations with limited many_to_many associations should work correctly" do
    Album.send :many_to_many, :first_two_tags, {:clone=>:first_two_tags}.merge(@els)
    Album.send :many_to_many, :second_two_tags, {:clone=>:second_two_tags}.merge(@els)
    Album.send :many_to_many, :not_first_tags, {:clone=>:not_first_tags}.merge(@els)
    Album.send :many_to_many, :last_two_tags, {:clone=>:last_two_tags}.merge(@els)
    tu, tv = @other_tags.call
    al = @pr.call.first
    al.add_tag(tu)
    ds = Album.order(:name)
    
    ds.where(@album.pk_hash).first_two_tags.all.should == [@tag, tu]
    ds.where(@album.pk_hash).second_two_tags.all.should == [tu, tv]
    ds.where(@album.pk_hash).not_first_tags.all.should == [tu, tv]
    ds.where(@album.pk_hash).last_two_tags.all.should == [tv, tu]
    ds.where(al.pk_hash).first_two_tags.all.should == [tu]
    ds.where(al.pk_hash).second_two_tags.all.should == []
    ds.where(al.pk_hash).not_first_tags.all.should == []
    ds.where(al.pk_hash).last_two_tags.all.should == [tu]

    Album.many_to_many :first_two_tags, :clone=>:first_two_tags do |ads| ads.where(:tags__name=>tu.name) end
    Album.many_to_many :second_two_tags, :clone=>:second_two_tags do |ads| ads.where(:tags__name=>[tu.name, tv.name]) end

    ds.where(@album.pk_hash).first_two_tags.all.should == [tu]
    ds.where(@album.pk_hash).second_two_tags.all.should == [tv]
    ds.where(al.pk_hash).first_two_tags.all.should == [tu]
    ds.where(al.pk_hash).second_two_tags.all.should == []

    al.add_tag(tv)
    ds.where(@album.pk_hash).first_two_tags.all.should == [tu]
    ds.where(@album.pk_hash).second_two_tags.all.should == [tv]
    ds.where(al.pk_hash).first_two_tags.all.should == [tu]
    ds.where(al.pk_hash).second_two_tags.all.should == [tv]
  end

  specify "filter by associations with limited many_through_many associations should work correctly" do
    Artist.many_through_many :first_two_tags, {:clone=>:first_two_tags}.merge(@els)
    Artist.many_through_many :second_two_tags, {:clone=>:second_two_tags}.merge(@els)
    Artist.many_through_many :not_first_tags, {:clone=>:not_first_tags}.merge(@els)
    Artist.many_through_many :last_two_tags, {:clone=>:last_two_tags}.merge(@els)
    @album.update(:artist => @artist)
    tu, tv = @other_tags.call
    al, ar, _ = @pr.call
    al.update(:artist=>ar)
    al.add_tag(tu)
    ds = Artist.order(:name)
    
    ds.where(:first_two_tags=>@tag).all.should == [@artist]
    ds.where(:first_two_tags=>tu).all.should == [@artist, ar]
    ds.where(:first_two_tags=>tv).all.should == []
    ds.exclude(:first_two_tags=>@tag).all.should == [ar]
    ds.exclude(:first_two_tags=>tu).all.should == []
    ds.exclude(:first_two_tags=>tv).all.should == [@artist, ar]

    ds.where(:second_two_tags=>@tag).all.should == []
    ds.where(:second_two_tags=>tu).all.should == [@artist]
    ds.where(:second_two_tags=>tv).all.should == [@artist]
    ds.exclude(:second_two_tags=>@tag).all.should == [@artist, ar]
    ds.exclude(:second_two_tags=>tu).all.should == [ar]
    ds.exclude(:second_two_tags=>tv).all.should == [ar]

    ds.where(:not_first_tags=>@tag).all.should == []
    ds.where(:not_first_tags=>tu).all.should == [@artist]
    ds.where(:not_first_tags=>tv).all.should == [@artist]
    ds.exclude(:not_first_tags=>@tag).all.should == [@artist, ar]
    ds.exclude(:not_first_tags=>tu).all.should == [ar]
    ds.exclude(:not_first_tags=>tv).all.should == [ar]

    ds.where(:last_two_tags=>@tag).all.should == []
    ds.where(:last_two_tags=>tu).all.should == [@artist, ar]
    ds.where(:last_two_tags=>tv).all.should == [@artist]
    ds.exclude(:last_two_tags=>@tag).all.should == [@artist, ar]
    ds.exclude(:last_two_tags=>tu).all.should == []
    ds.exclude(:last_two_tags=>tv).all.should == [ar]

    Artist.many_through_many :first_two_tags, :clone=>:first_tag do |ads| ads.where(:tags__name=>tu.name) end
    Artist.many_through_many :second_two_tags, :clone=>:first_tag do |ads| ads.where(:tags__name=>[tv.name, tu.name]) end

    ds.where(:first_two_tags=>[@tag, tu]).all.should == [@artist, ar]
    ds.exclude(:first_two_tags=>[@tag, tu]).all.should == []

    al.add_tag(tv)
    ds.where(:second_two_tags=>[tv, tu]).all.should == [@artist, ar]
    ds.exclude(:second_two_tags=>[tv, tu]).all.should == []
  end

  specify "dataset associations with limited many_through_many associations should work correctly" do
    Artist.many_through_many :first_two_tags, {:clone=>:first_two_tags}.merge(@els)
    Artist.many_through_many :second_two_tags, {:clone=>:second_two_tags}.merge(@els)
    Artist.many_through_many :not_first_tags, {:clone=>:not_first_tags}.merge(@els)
    Artist.many_through_many :last_two_tags, {:clone=>:last_two_tags}.merge(@els)
    @album.update(:artist => @artist)
    tu, tv = @other_tags.call
    al, ar, _ = @pr.call
    al.update(:artist=>ar)
    al.add_tag(tu)
    ds = Artist.order(:name)
    
    ds.where(@artist.pk_hash).first_two_tags.all.should == [@tag, tu]
    ds.where(@artist.pk_hash).second_two_tags.all.should == [tu, tv]
    ds.where(@artist.pk_hash).not_first_tags.all.should == [tu, tv]
    ds.where(@artist.pk_hash).last_two_tags.all.should == [tv, tu]
    ds.where(ar.pk_hash).first_two_tags.all.should == [tu]
    ds.where(ar.pk_hash).second_two_tags.all.should == []
    ds.where(ar.pk_hash).not_first_tags.all.should == []
    ds.where(ar.pk_hash).last_two_tags.all.should == [tu]

    Artist.many_through_many :first_two_tags, :clone=>:first_two_tags do |ads| ads.where(:tags__name=>tu.name) end
    Artist.many_through_many :second_two_tags, :clone=>:second_two_tags do |ads| ads.where(:tags__name=>[tu.name, tv.name]) end

    ds.where(@artist.pk_hash).first_two_tags.all.should == [tu]
    ds.where(@artist.pk_hash).second_two_tags.all.should == [tv]
    ds.where(ar.pk_hash).first_two_tags.all.should == [tu]
    ds.where(ar.pk_hash).second_two_tags.all.should == []

    al.add_tag(tv)
    ds.where(@artist.pk_hash).first_two_tags.all.should == [tu]
    ds.where(@artist.pk_hash).second_two_tags.all.should == [tv]
    ds.where(ar.pk_hash).first_two_tags.all.should == [tu]
    ds.where(ar.pk_hash).second_two_tags.all.should == [tv]
  end
end

shared_examples_for "basic regular and composite key associations" do  
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
    unless @no_many_through_many
      Album.first_tags.all.should == []
      Artist.tags.all.should == []
      Artist.first_tags.all.should == []
    end
    Artist.albums.tags.all.should == []

    @album.update(:artist => @artist)
    @album.add_tag(@tag)

    Tag.albums.all.should == [@album]
    Album.artists.all.should == [@artist]
    Album.tags.all.should == [@tag]
    Album.alias_tags.all.should == [@tag]
    Artist.albums.all.should == [@album]
    unless @no_many_through_many
      Album.first_tags.all.should == [@tag]
      Artist.tags.all.should == [@tag]
      Artist.first_tags.all.should == [@tag]
    end
    Artist.albums.tags.all.should == [@tag]

    album.add_tag(tag)
    album.update(:artist => artist)

    Tag.albums.order(:name).all.should == [@album, album]
    Album.artists.order(:name).all.should == [@artist, artist]
    Album.tags.order(:name).all.should == [@tag, tag]
    Album.alias_tags.order(:name).all.should == [@tag, tag]
    Artist.albums.order(:name).all.should == [@album, album]
    unless @no_many_through_many
      Album.first_tags.order(:name).all.should == [@tag, tag]
      Artist.tags.order(:name).all.should == [@tag, tag]
      Artist.first_tags.order(:name).all.should == [@tag, tag]
    end
    Artist.albums.tags.order(:name).all.should == [@tag, tag]

    Tag.filter(Tag.qualified_primary_key_hash(tag.pk)).albums.all.should == [album]
    Album.filter(Album.qualified_primary_key_hash(album.pk)).artists.all.should == [artist]
    Album.filter(Album.qualified_primary_key_hash(album.pk)).tags.all.should == [tag]
    Album.filter(Album.qualified_primary_key_hash(album.pk)).alias_tags.all.should == [tag]
    Artist.filter(Artist.qualified_primary_key_hash(artist.pk)).albums.all.should == [album]
    unless @no_many_through_many
      Album.filter(Album.qualified_primary_key_hash(album.pk)).first_tags.all.should == [tag]
      Artist.filter(Artist.qualified_primary_key_hash(artist.pk)).tags.all.should == [tag]
      Artist.filter(Artist.qualified_primary_key_hash(artist.pk)).first_tags.all.should == [tag]
    end
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
    
    @album.remove_all_tags
    @artist.remove_all_albums
    
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
  
  describe "when filtering/excluding by associations" do
    before do
      @Artist = Artist.dataset
      @Album = Album.dataset
      @Tag = Tag.dataset
    end

    it_should_behave_like "filtering/excluding by associations"
  end
end

shared_examples_for "regular and composite key associations" do  
  it_should_behave_like "basic regular and composite key associations"

  describe "when filtering/excluding by associations when joining" do
    def self_join(c)
      c.join(Sequel.as(c.table_name, :b), Array(c.primary_key).zip(Array(c.primary_key))).select_all(c.table_name)
    end

    before do
      @Artist = self_join(Artist)
      @Album = self_join(Album)
      @Tag = self_join(Tag)
    end

    it_should_behave_like "filtering/excluding by associations"
  end

  describe "with default/union :eager_limit_strategy" do
    before do
      @els = {}
    end
    it_should_behave_like "eager limit strategies"
  end

  describe "with :eager_limit_strategy=>:ruby" do
    before do
      @els = {:eager_limit_strategy=>:ruby}
    end
    it_should_behave_like "eager limit strategies"
    it_should_behave_like "eager_graph limit strategies"
  end

  describe "with :eager_limit_strategy=>:distinct_on" do
    before do
      @els = {:eager_limit_strategy=>:distinct_on}
    end
    it_should_behave_like "one_to_one eager limit strategies"
    it_should_behave_like "one_through_one eager limit strategies"
    it_should_behave_like "one_through_many eager limit strategies"
    it_should_behave_like "one_to_one eager_graph limit strategies"
    it_should_behave_like "one_through_one eager_graph limit strategies"
    it_should_behave_like "one_through_many eager_graph limit strategies"
    it_should_behave_like "filter by associations singular association limit strategies"
  end if DB.dataset.supports_ordered_distinct_on?

  describe "with :eager_limit_strategy=>:window_function" do
    before do
      @els = {:eager_limit_strategy=>:window_function}
    end
    it_should_behave_like "eager limit strategies"
    it_should_behave_like "eager_graph limit strategies"
    it_should_behave_like "filter by associations limit strategies"
  end if DB.dataset.supports_window_functions?

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

  specify "should work with a one_through_many association" do
    @album.update(:artist => @artist)
    @album.add_tag(@tag)

    @album.reload
    @artist.reload
    @tag.reload
    
    @album.tags.should == [@tag]
    
    a = Artist.eager(:first_tag).all
    a.should == [@artist]
    a.first.first_tag.should == @tag
    
    a = Artist.eager_graph(:first_tag).all
    a.should == [@artist]
    a.first.first_tag.should == @tag
    
    a = Album.eager(:artist=>:first_tag).all
    a.should == [@album]
    a.first.artist.should == @artist
    a.first.artist.first_tag.should == @tag
    
    a = Album.eager_graph(:artist=>:first_tag).all
    a.should == [@album]
    a.first.artist.should == @artist
    a.first.artist.first_tag.should == @tag
  end
end

describe "Sequel::Model Simple Associations" do
  before(:all) do
    @db = DB
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
      one_to_one :first_album, :clone=>:albums
      one_to_one :second_album, :clone=>:albums, :limit=>[nil, 1]
      one_to_one :last_album, :class=>:Album, :order=>Sequel.desc(:name)
      one_to_many :first_two_albums, :clone=>:albums, :limit=>2
      one_to_many :second_two_albums, :clone=>:albums, :limit=>[2, 1]
      one_to_many :not_first_albums, :clone=>:albums, :limit=>[nil, 1]
      one_to_many :last_two_albums, :class=>:Album, :order=>Sequel.desc(:name), :limit=>2
      one_to_many :a_albums, :clone=>:albums, :conditions=>{:name=>'Al'}
      one_to_one :first_a_album, :clone=>:a_albums
      plugin :many_through_many
      many_through_many :tags, [[:albums, :artist_id, :id], [:albums_tags, :album_id, :tag_id]]
      many_through_many :first_two_tags, :clone=>:tags, :order=>:tags__name, :limit=>2, :graph_order=>:name
      many_through_many :second_two_tags, :clone=>:tags, :order=>:tags__name, :limit=>[2, 1], :graph_order=>:name
      many_through_many :not_first_tags, :clone=>:tags, :order=>:tags__name, :limit=>[nil, 1], :graph_order=>:name
      many_through_many :last_two_tags, :clone=>:tags, :order=>Sequel.desc(:tags__name), :limit=>2, :graph_order=>Sequel.desc(:name)
      many_through_many :t_tags, :clone=>:tags, :conditions=>{:tags__name=>'T'}
      one_through_many :first_tag, [[:albums, :artist_id, :id], [:albums_tags, :album_id, :tag_id]], :order=>:tags__name, :graph_order=>:name, :class=>:Tag
      one_through_many :second_tag, :clone=>:first_tag, :limit=>[nil, 1]
      one_through_many :last_tag, :clone=>:first_tag, :order=>Sequel.desc(:tags__name), :graph_order=>Sequel.desc(:name)
      one_through_many :t_tag, :clone=>:first_tag, :conditions=>{:tags__name=>'T'}
    end
    class ::Album < Sequel::Model(@db)
      plugin :dataset_associations
      many_to_one :artist, :reciprocal=>nil
      many_to_one :a_artist, :clone=>:artist, :conditions=>{:name=>'Ar'}, :key=>:artist_id
      many_to_many :tags, :right_key=>:tag_id
      many_to_many :alias_tags, :clone=>:tags, :join_table=>:albums_tags___at
      many_to_many :first_two_tags, :clone=>:tags, :order=>:name, :limit=>2
      many_to_many :second_two_tags, :clone=>:tags, :order=>:name, :limit=>[2, 1]
      many_to_many :not_first_tags, :clone=>:tags, :order=>:name, :limit=>[nil, 1]
      many_to_many :last_two_tags, :clone=>:tags, :order=>Sequel.desc(:name), :limit=>2
      many_to_many :t_tags, :clone=>:tags, :conditions=>{:name=>'T'}
      many_to_many :alias_t_tags, :clone=>:t_tags, :join_table=>:albums_tags___at
      one_through_one :first_tag, :clone=>:tags, :order=>:name
      one_through_one :second_tag, :clone=>:first_tag, :limit=>[nil, 1]
      one_through_one :last_tag, :clone=>:tags, :order=>Sequel.desc(:name)
      one_through_one :t_tag, :clone=>:t_tags
      one_through_one :alias_t_tag, :clone=>:alias_t_tags
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

  describe "with :correlated_subquery limit strategy" do
    before do
      @els = {:eager_limit_strategy=>:correlated_subquery}
    end

    it_should_behave_like "one_to_one eager_graph limit strategies"
    it_should_behave_like "one_to_many eager_graph limit strategies"
    it_should_behave_like "filter by associations one_to_one limit strategies"
    it_should_behave_like "filter by associations one_to_many limit strategies"
  end if DB.dataset.supports_limits_in_correlated_subqueries?

  specify "should handle eager loading limited associations for many objects" do
    @db[:artists].import([:name], (1..99).map{|i| [i.to_s]})
    artists = Artist.eager(:albums).all
    artists.length.should == 100
    artists.each{|a| a.albums.should == []}
    artists = Artist.eager(:first_two_albums).all
    artists.length.should == 100
    artists.each{|a| a.first_two_albums.should == []}
    @db[:albums].insert([:artist_id], @db[:artists].select(:id))
    artists = Artist.eager(:albums).all
    artists.length.should == 100
    artists.each{|a| a.albums.length.should == 1}
    artists = Artist.eager(:first_two_albums).all
    artists.length.should == 100
    artists.each{|a| a.first_two_albums.length.should == 1}
  end

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
    Artist.one_to_many :balbums, :class=>Album, :key=>:artist_id, :reciprocal=>nil
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
    Album.dataset.delete
    @album = @artist.add_album(:name=>'Al2')
    Album.first[:name].should == 'Al2'
    @artist.albums_dataset.first[:name].should == 'Al2'
    
    @album.remove_all_tags
    Tag.dataset.delete
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
    @db = DB
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
      set_primary_key [:id1, :id2]
      unrestrict_primary_key
      one_to_many :albums, :key=>[:artist_id1, :artist_id2], :order=>:name
      one_to_one :first_album, :clone=>:albums
      one_to_one :last_album, :clone=>:albums, :order=>Sequel.desc(:name)
      one_to_one :second_album, :clone=>:albums, :limit=>[nil, 1]
      one_to_many :first_two_albums, :clone=>:albums, :order=>:name, :limit=>2
      one_to_many :second_two_albums, :clone=>:albums, :order=>:name, :limit=>[2, 1]
      one_to_many :not_first_albums, :clone=>:albums, :order=>:name, :limit=>[nil, 1]
      one_to_many :last_two_albums, :clone=>:albums, :order=>Sequel.desc(:name), :limit=>2
      one_to_many :a_albums, :clone=>:albums do |ds| ds.where(:name=>'Al') end
      one_to_one :first_a_album, :clone=>:a_albums
      plugin :many_through_many
      many_through_many :tags, [[:albums, [:artist_id1, :artist_id2], [:id1, :id2]], [:albums_tags, [:album_id1, :album_id2], [:tag_id1, :tag_id2]]]
      many_through_many :first_two_tags, :clone=>:tags, :order=>:tags__name, :limit=>2, :graph_order=>:name
      many_through_many :second_two_tags, :clone=>:tags, :order=>:tags__name, :limit=>[2, 1], :graph_order=>:name
      many_through_many :not_first_tags, :clone=>:tags, :order=>:tags__name, :limit=>[nil, 1], :graph_order=>:name
      many_through_many :last_two_tags, :clone=>:tags, :order=>Sequel.desc(:tags__name), :limit=>2, :graph_order=>Sequel.desc(:name)
      many_through_many :t_tags, :clone=>:tags do |ds| ds.where(:tags__name=>'T') end
      one_through_many :first_tag, [[:albums, [:artist_id1, :artist_id2], [:id1, :id2]], [:albums_tags, [:album_id1, :album_id2], [:tag_id1, :tag_id2]]], :order=>:tags__name, :graph_order=>:name, :class=>:Tag
      one_through_many :second_tag, :clone=>:first_tag, :limit=>[nil, 1]
      one_through_many :last_tag, :clone=>:first_tag, :order=>Sequel.desc(:tags__name), :graph_order=>Sequel.desc(:name)
      one_through_many :t_tag, :clone=>:first_tag do |ds| ds.where(:tags__name=>'T') end
    end
    class ::Album < Sequel::Model(@db)
      plugin :dataset_associations
      set_primary_key [:id1, :id2]
      unrestrict_primary_key
      many_to_one :artist, :key=>[:artist_id1, :artist_id2], :reciprocal=>nil
      many_to_one(:a_artist, :clone=>:artist){|ds| ds.where(:name=>'Ar')}
      many_to_many :tags, :left_key=>[:album_id1, :album_id2], :right_key=>[:tag_id1, :tag_id2]
      many_to_many :alias_tags, :clone=>:tags, :join_table=>:albums_tags___at
      many_to_many :first_two_tags, :clone=>:tags, :order=>:name, :limit=>2
      many_to_many :second_two_tags, :clone=>:tags, :order=>:name, :limit=>[2, 1]
      many_to_many :not_first_tags, :clone=>:tags, :order=>:name, :limit=>[nil, 1]
      many_to_many :last_two_tags, :clone=>:tags, :order=>Sequel.desc(:name), :limit=>2
      many_to_many :t_tags, :clone=>:tags do |ds| ds.where(:name=>'T') end
      many_to_many :alias_t_tags, :clone=>:t_tags, :join_table=>:albums_tags___at
      one_through_one :first_tag, :clone=>:tags, :order=>:name
      one_through_one :second_tag, :clone=>:first_tag, :limit=>[nil, 1]
      one_through_one :last_tag, :clone=>:tags, :order=>Sequel.desc(:name)
      one_through_one :t_tag, :clone=>:t_tags
      one_through_one :alias_t_tag, :clone=>:alias_t_tags
    end
    class ::Tag < Sequel::Model(@db)
      plugin :dataset_associations
      set_primary_key [:id1, :id2]
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

  describe "with :correlated_subquery limit strategy" do
    before do
      @els = {:eager_limit_strategy=>:correlated_subquery}
    end

    it_should_behave_like "one_to_one eager_graph limit strategies"
    it_should_behave_like "one_to_many eager_graph limit strategies"
    it_should_behave_like "filter by associations one_to_one limit strategies"
    it_should_behave_like "filter by associations one_to_many limit strategies"
  end if DB.dataset.supports_limits_in_correlated_subqueries? && DB.dataset.supports_multiple_column_in?

  specify "should have add method accept hashes and create new records" do
    @artist.remove_all_albums
    Album.dataset.delete
    @artist.add_album(:id1=>1, :id2=>2, :name=>'Al2')
    Album.first[:name].should == 'Al2'
    @artist.albums_dataset.first[:name].should == 'Al2'
    
    @album.remove_all_tags
    Tag.dataset.delete
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

describe "Sequel::Model pg_array_to_many" do
  before(:all) do
    @db = DB
    @db.extension :pg_array
    Sequel.extension :pg_array_ops
    @db.drop_table?(:tags, :albums, :artists)
    @db.create_table(:artists) do
      primary_key :id
      String :name
    end
    @db.create_table(:albums) do
      primary_key :id
      String :name
      foreign_key :artist_id, :artists
      column :tag_ids, 'int4[]'
    end
    @db.create_table(:tags) do
      primary_key :id
      String :name
    end
  end
  before do
    [:tags, :albums, :artists].each{|t| @db[t].delete}
    class ::Artist < Sequel::Model(@db)
      plugin :dataset_associations
      one_to_many :albums, :order=>:name
      one_to_one :first_album, :clone=>:albums
      one_to_many :a_albums, :clone=>:albums do |ds| ds.where(:name=>'Al') end
      one_to_one :first_a_album, :clone=>:a_albums
    end
    class ::Album < Sequel::Model(@db)
      plugin :dataset_associations
      plugin :pg_array_associations
      many_to_one :artist, :reciprocal=>nil
      many_to_one :a_artist, :clone=>:artist, :key=>:artist_id do |ds| ds.where(:name=>'Ar') end
      pg_array_to_many :tags, :key=>:tag_ids, :save_after_modify=>true
      pg_array_to_many :alias_tags, :clone=>:tags
      pg_array_to_many :first_two_tags, :clone=>:tags, :order=>:name, :limit=>2
      pg_array_to_many :second_two_tags, :clone=>:tags, :order=>:name, :limit=>[2, 1]
      pg_array_to_many :not_first_tags, :clone=>:tags, :order=>:name, :limit=>[nil, 1]
      pg_array_to_many :last_two_tags, :clone=>:tags, :order=>Sequel.desc(:name), :limit=>2
      pg_array_to_many :t_tags, :clone=>:tags do |ds| ds.where(:tags__name=>'T') end
      pg_array_to_many :alias_t_tags, :clone=>:t_tags
    end
    class ::Tag < Sequel::Model(@db)
      plugin :dataset_associations
      plugin :pg_array_associations
      many_to_pg_array :albums
    end
    @album = Album.create(:name=>'Al')
    @artist = Artist.create(:name=>'Ar')
    @tag = Tag.create(:name=>'T')
    @many_to_many_method = :pg_array_to_many
    @no_many_through_many = true
    @same_album = lambda{Album.create(:name=>'Al', :artist_id=>@artist.id)}
    @diff_album = lambda{Album.create(:name=>'lA', :artist_id=>@artist.id)}
    @middle_album = lambda{Album.create(:name=>'Bl', :artist_id=>@artist.id)}
    @other_tags = lambda{t = [Tag.create(:name=>'U'), Tag.create(:name=>'V')]; Tag.all{|x| @album.add_tag(x)}; t}
    @pr = lambda{[Album.create(:name=>'Al2'),Artist.create(:name=>'Ar2'),Tag.create(:name=>'T2')]}
    @ins = lambda{}
  end
  after do
    [:Tag, :Album, :Artist].each{|x| Object.send(:remove_const, x)}
  end
  after(:all) do
    @db.drop_table?(:tags, :albums, :artists)
  end
  
  it_should_behave_like "basic regular and composite key associations"
  it_should_behave_like "many_to_many eager limit strategies"
  it_should_behave_like "many_to_many eager_graph limit strategies"

  it "should handle adding and removing entries in array" do
    a = Album.create
    a.typecast_on_assignment = false
    a.add_tag(@tag)
    a.remove_tag(@tag)
    a.save
  end
end if DB.database_type == :postgres && [:postgres, :jdbc].include?(DB.adapter_scheme) && DB.server_version >= 90300

describe "Sequel::Model many_to_pg_array" do
  before(:all) do
    @db = DB
    @db.extension :pg_array
    Sequel.extension :pg_array_ops
    @db.drop_table?(:tags, :albums, :artists)
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
      column :album_ids, 'int4[]'
    end
  end
  before do
    [:tags, :albums, :artists].each{|t| @db[t].delete}
    class ::Artist < Sequel::Model(@db)
      plugin :dataset_associations
      one_to_many :albums, :order=>:name
      one_to_one :first_album, :class=>:Album, :order=>:name
      one_to_many :a_albums, :clone=>:albums do |ds| ds.where(:name=>'Al') end
      one_to_one :first_a_album, :clone=>:a_albums
    end
    class ::Album < Sequel::Model(@db)
      plugin :dataset_associations
      plugin :pg_array_associations
      many_to_one :artist, :reciprocal=>nil
      many_to_one :a_artist, :clone=>:artist, :key=>:artist_id do |ds| ds.where(:name=>'Ar') end
      many_to_pg_array :tags
      many_to_pg_array :alias_tags, :clone=>:tags
      many_to_pg_array :first_two_tags, :clone=>:tags, :order=>:name, :limit=>2
      many_to_pg_array :second_two_tags, :clone=>:tags, :order=>:name, :limit=>[2, 1]
      many_to_pg_array :not_first_tags, :clone=>:tags, :order=>:name, :limit=>[nil, 1]
      many_to_pg_array :last_two_tags, :clone=>:tags, :order=>Sequel.desc(:name), :limit=>2
      many_to_pg_array :t_tags, :clone=>:tags do |ds| ds.where(:tags__name=>'T') end
      many_to_pg_array :alias_t_tags, :clone=>:t_tags
    end
    class ::Tag < Sequel::Model(@db)
      plugin :dataset_associations
      plugin :pg_array_associations
      pg_array_to_many :albums, :save_after_modify=>true
    end
    @album = Album.create(:name=>'Al')
    @artist = Artist.create(:name=>'Ar')
    @tag = Tag.create(:name=>'T')
    @many_to_many_method = :pg_array_to_many
    @no_many_through_many = true
    @same_album = lambda{Album.create(:name=>'Al', :artist_id=>@artist.id)}
    @diff_album = lambda{Album.create(:name=>'lA', :artist_id=>@artist.id)}
    @middle_album = lambda{Album.create(:name=>'Bl', :artist_id=>@artist.id)}
    @other_tags = lambda{t = [Tag.create(:name=>'U'), Tag.create(:name=>'V')]; Tag.all{|x| @album.add_tag(x)}; @tag.refresh; t.each{|x| x.refresh}; t}
    @pr = lambda{[Album.create(:name=>'Al2'),Artist.create(:name=>'Ar2'),Tag.create(:name=>'T2')]}
    @ins = lambda{}
  end
  after do
    [:Tag, :Album, :Artist].each{|x| Object.send(:remove_const, x)}
  end
  after(:all) do
    @db.drop_table?(:tags, :albums, :artists)
  end
  
  it_should_behave_like "basic regular and composite key associations"
  it_should_behave_like "many_to_many eager limit strategies"
  it_should_behave_like "many_to_many eager_graph limit strategies"

  it "should handle adding and removing entries in array" do
    a = Album.create
    @tag.typecast_on_assignment = false
    a.add_tag(@tag)
    a.remove_tag(@tag)
  end
end if DB.database_type == :postgres && [:postgres, :jdbc].include?(DB.adapter_scheme) && DB.server_version >= 90300

describe "Sequel::Model Associations with clashing column names" do
  before(:all) do
    @db = DB
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
    @Foo.one_through_one :mtmbar, :join_table=>:bars_foos, :left_primary_key=>:obj_id, :left_primary_key_column=>:object_id, :right_primary_key=>:object_id, :right_primary_key_method=>:obj_id, :left_key=>:foo_id, :right_key=>:object_id, :class=>@Bar
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
    @Foo.first.mtmbar.should == @bar
    @Bar.first.mtmfoos.should == [@foo]
  end

  it "should have working eager loading methods" do
    @Bar.eager(:foo).all.map{|o| [o, o.foo]}.should == [[@bar, @foo]]
    @Foo.eager(:bars).all.map{|o| [o, o.bars]}.should == [[@foo, [@bar]]]
    @Foo.eager(:bar).all.map{|o| [o, o.bar]}.should == [[@foo, @bar]]
    @Foo.eager(:mtmbars).all.map{|o| [o, o.mtmbars]}.should == [[@foo, [@bar]]]
    @Foo.eager(:mtmbar).all.map{|o| [o, o.mtmbar]}.should == [[@foo, @bar]]
    @Bar.eager(:mtmfoos).all.map{|o| [o, o.mtmfoos]}.should == [[@bar, [@foo]]]
  end

  it "should have working eager graphing methods" do
    @Bar.eager_graph(:foo).all.map{|o| [o, o.foo]}.should == [[@bar, @foo]]
    @Foo.eager_graph(:bars).all.map{|o| [o, o.bars]}.should == [[@foo, [@bar]]]
    @Foo.eager_graph(:bar).all.map{|o| [o, o.bar]}.should == [[@foo, @bar]]
    @Foo.eager_graph(:mtmbars).all.map{|o| [o, o.mtmbars]}.should == [[@foo, [@bar]]]
    @Foo.eager_graph(:mtmbar).all.map{|o| [o, o.mtmbar]}.should == [[@foo, @bar]]
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
    @foo.bars.sort_by{|x| x.id}.should == [@bar, b]
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
