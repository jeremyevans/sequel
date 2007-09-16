module Sequel
  class Model
    ONE_TO_ONE_PROC = "proc {i = @values[:%s]; %s[i] if i}".freeze
    ID_POSTFIX = "_id".freeze
    FROM_DATASET = "db[%s]".freeze
    
    def self.one_to_one(name, opts)
      klass = opts[:class] ? opts[:class] : (FROM_DATASET % name.inspect)
      key = opts[:key] || (name.to_s + ID_POSTFIX)
      define_method name, &eval(ONE_TO_ONE_PROC % [key, klass])
    end
  
    ONE_TO_MANY_PROC = "proc {%s.filter(:%s => pkey)}".freeze
    ONE_TO_MANY_ORDER_PROC = "proc {%s.filter(:%s => pkey).order(%s)}".freeze
    def self.one_to_many(name, opts)
      klass = opts[:class] ? opts[:class] :
        (FROM_DATASET % (opts[:table] || name.inspect))
      key = opts[:on]
      order = opts[:order]
      define_method name, &eval(
        (order ? ONE_TO_MANY_ORDER_PROC : ONE_TO_MANY_PROC) %
        [klass, key, order.inspect]
      )
    end
  end
end