module Sequel
  class Dataset
    def create_view(name)
      @db.create_view(name, self)
    end

    def create_or_replace_view(name)
      @db.create_or_replace_view(name, self)
    end
  end
end
