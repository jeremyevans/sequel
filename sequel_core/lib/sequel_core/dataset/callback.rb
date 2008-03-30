module Sequel
  class Dataset
    module Callback
      # Add a symbol with the name of a method to the list of callbacks for name.
      def add_callback(name, sym)
        cbs = (@opts[:callbacks] ||= {})
        cb = (cbs[name] ||= [])
        cb.push(sym)
      end
      
      # Run all callbacks for name with the given args
      def run_callback(name, *args)
        return unless cbs = @opts[:callbacks]
        return unless cb = cbs[name]
        cb.each{|sym| send(sym, *args)}
      end
    end
  end
end
