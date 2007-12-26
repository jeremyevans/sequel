module Sequel
  class Model
    def self.database_opened(db)
      @db = db if (self == Model) && !@db
    end
  end
end

