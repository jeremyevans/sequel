module Sequel
  # A dataset represents an SQL query, or more generally, an abstract
  # set of rows in the database.  Datasets
  # can be used to create, retrieve, update and delete records.
  # 
  # Query results are always retrieved on demand, so a dataset can be kept
  # around and reused indefinitely (datasets never cache results):
  #
  #   my_posts = DB[:posts].filter(:author => 'david') # no records are retrieved
  #   my_posts.all # records are retrieved
  #   my_posts.all # records are retrieved again
  #
  # Most dataset methods return modified copies of the dataset (functional style), so you can
  # reuse different datasets to access data:
  #
  #   posts = DB[:posts]
  #   davids_posts = posts.filter(:author => 'david')
  #   old_posts = posts.filter('stamp < ?', Date.today - 7)
  #   davids_old_posts = davids_posts.filter('stamp < ?', Date.today - 7)
  #
  # Datasets are Enumerable objects, so they can be manipulated using any
  # of the Enumerable methods, such as map, inject, etc.
  #
  # For more information, see the {"Dataset Basics" guide}[rdoc-ref:doc/dataset_basics.rdoc].
  class Dataset
    OPTS = Sequel::OPTS

    include Enumerable
    include SQL::AliasMethods
    include SQL::BooleanMethods
    include SQL::CastMethods
    include SQL::ComplexExpressionMethods
    include SQL::InequalityMethods
    include SQL::NumericMethods
    include SQL::OrderMethods
    include SQL::StringMethods
  end
  
  require(%w"query actions features graph prepared_statements misc mutation sql placeholder_literalizer", 'dataset')
end
