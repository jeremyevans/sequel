module Sequel
  class Dataset
    # Creates a view in the database with the given named based
    # on the current dataset.
    def create_view(name)
      @db.create_view(name, self)
    end

    # Creates or replaces a view in the database with the given
    # named based on the current dataset.
    def create_or_replace_view(name)
      @db.create_or_replace_view(name, self)
    end
  end
end
