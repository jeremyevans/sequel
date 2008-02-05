module Sequel
  class Model
    
    # Creates the relationships block which helps you organize your relationships in your model
    # 
    #   class Post
    #     relationships do
    #       has :many, :comments
    #     end
    #   end
    def self.relationships(&block)
      RelationshipsBlock::Generator.new(self, &block) if block_given?
      @relationships
    end
    
    module RelationshipsBlock
      class Generator
        def initialize(model_class, &block)
          @model_class = model_class
          instance_eval(&block)
        end

        def method_missing(method, *args)
          @model_class.send(method, *args)
        end
      end
    end
    
  end
end
