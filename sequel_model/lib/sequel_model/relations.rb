module Sequel
  class Model
    ID_POSTFIX = '_id'.freeze
    
    # Creates a 1-1 relationship by defining an association method, e.g.:
    # 
    #   class Session < Sequel::Model(:sessions)
    #   end
    #
    #   class Node < Sequel::Model(:nodes)
    #     one_to_one :producer, :from => Session
    #     # which is equivalent to
    #     def producer
    #       Session[producer_id] if producer_id
    #     end
    #   end
    #
    # You can also set the foreign key explicitly by including a :key option:
    #
    #   one_to_one :producer, :from => Session, :key => :producer_id
    #
    # The one_to_one macro also creates a setter, which accepts nil, a hash or
    # a model instance, e.g.:
    #
    #   p = Producer[1234]
    #   node = Node[:path => '/']
    #   node.producer = p
    #   node.producer_id #=> 1234
    #
    def self.one_to_one(name, opts)
      from = opts[:from]
      from || (raise Error, "No association source defined (use :from option)")
      key = opts[:key] || (name.to_s + ID_POSTFIX).to_sym
      
      setter_name = "#{name}=".to_sym
      
      case from
      when Symbol
        class_def(name) {(k = @values[key]) ? db[from][:id => k] : nil}
      when Sequel::Dataset
        class_def(name) {(k = @values[key]) ? from[:id => k] : nil}
      else
        class_def(name) {(k = @values[key]) ? from[k] : nil}
      end
      class_def(setter_name) do |v|
        case v
        when nil
          set(key => nil)
        when Sequel::Model
          set(key => v.pk)
        when Hash
          set(key => v[:id])
        end
      end

      # define_method name, &eval(ONE_TO_ONE_PROC % [key, from])
    end
  
    # Creates a 1-N relationship by defining an association method, e.g.:
    # 
    #   class Book < Sequel::Model(:books)
    #   end
    #
    #   class Author < Sequel::Model(:authors)
    #     one_to_many :books, :from => Book
    #     # which is equivalent to
    #     def books
    #       Book.filter(:author_id => id)
    #     end
    #   end
    #
    # You can also set the foreign key explicitly by including a :key option:
    #
    #   one_to_many :books, :from => Book, :key => :author_id
    #
    def self.one_to_many(name, opts)
      from = opts[:from]
      from || (raise Error, "No association source defined (use :from option)")
      key = opts[:key] || (self.to_s + ID_POSTFIX).to_sym
      
      case from
      when Symbol
        class_def(name) {db[from].filter(key => pk)}
      else
        class_def(name) {from.filter(key => pk)}
      end
    end
  end
end