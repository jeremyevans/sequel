# frozen-string-literal: true

module Sequel
  extension 'async_thread_pool'

  module Plugins
    # The async_thread_pool plugin makes it slightly easier to use the async_thread_pool
    # Database extension with models.  It makes Model.async return an async dataset for the
    # model, and support async behavior for #destroy, #with_pk, and #with_pk! for model
    # datasets:
    #
    #   # Will load the artist with primary key 1 asynchronously
    #   artist = Artist.async.with_pk(1)
    #
    # You must load the async_thread_pool Database extension into the Database object the
    # model class uses in order for async behavior to work.
    #
    # Usage:
    #
    #   # Make all model subclass datasets support support async class methods and additional
    #   # async dataset methods
    #   Sequel::Model.plugin :async_thread_pool
    #
    #   # Make the Album class support async class method and additional async dataset methods
    #   Album.plugin :async_thread_pool
    module AsyncThreadPool
      module ClassMethods
        Plugins.def_dataset_methods(self, :async)
      end

      module DatasetMethods
        [:destroy, :with_pk, :with_pk!].each do |meth|
          ::Sequel::Database::AsyncThreadPool::DatasetMethods.define_async_method(self, meth)
        end
      end
    end
  end
end

