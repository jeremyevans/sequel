# frozen-string-literal: true

module Sequel
  module Plugins
    # The auto_restrict_eager_graph plugin will automatically disallow the use
    # of eager_graph for associations that have associated blocks but no :graph_*
    # association options.  The reason for this is the block will have an effect
    # during regular and eager loading, but not loading via eager_graph, and it
    # is likely that whatever the block is doing should have an equivalent done
    # when eager_graphing.  Most likely, not including a :graph_* option was either
    # an oversight (and one should be added), or use with eager_graph was never
    # intended (and usage should be forbidden).  Disallowing eager_graph in this
    # case prevents likely unexpected behavior during eager_graph.
    #
    # As an example of this, consider the following code:
    #
    #   Album.one_to_many :popular_tracks, class: :Track do |ds|
    #     ds = ds.where(popular: true)
    #   end  
    #
    #   Album.eager(:popular_tracks).all
    #   # SELECT * FROM albums
    #   # SELECT * FROM tracks WHERE ((popular IS TRUE) AND (album_id IN (...)))
    #
    #   # Notice that no condition for tracks.popular is added.
    #   Album.eager_graph(:popular_tracks).all
    #   # SELECT ... FROM albums LEFT JOIN tracks ON (tracks.album_id = albums.id)
    #   
    # With the auto_restrict_eager_graph plugin, the eager_graph call above will
    # raise an error, alerting you to the fact that you either should not be
    # using eager_graph with the association, or that you should be adding an
    # appropriate :graph_* option, such as:
    #
    #   Album.one_to_many :popular_tracks, class: :Track, graph_conditions: {popular: true} do |ds|
    #     ds = ds.where(popular: true)
    #   end  
    #
    # Usage:
    #
    #   # Automatically restrict eager_graph for associations if appropriate for all
    #   # model subclasses (called before loading subclasses)
    #   Sequel::Model.plugin :auto_restrict_eager_graph
    #
    #   # Automatically restrict eager_graph for associations in Album class
    #   Album.plugin :auto_restrict_eager_graph
    module AutoRestrictEagerGraph
      module ClassMethods
        # When defining an association, if a block is given for the association, but
        # a :graph_* option is not used, disallow the use of eager_graph.
        def associate(type, name, opts = OPTS, &block)
          opts = super

          if opts[:block] && !opts.has_key?(:allow_eager_graph) && !opts[:orig_opts].any?{|k,| /\Agraph_/ =~ k}
            opts[:allow_eager_graph] = false
          end

          opts
        end
      end
    end
  end
end
