# frozen-string-literal: true

module Sequel 
  class Dataset
    module StoredProcedureMethods
      # The name of the stored procedure to call
      attr_accessor :sproc_name
      
      # The name of the stored procedure to call
      attr_writer :sproc_args
      
      # Call the stored procedure with the given args
      def call(*args, &block)
        sp = clone
        sp.sproc_args = args
        sp.run(&block)
      end

      # Programmer friendly string showing this is a stored procedure,
      # showing the name of the procedure.
      def inspect
        "<#{self.class.name}/StoredProcedure name=#{@sproc_name}>"
      end
      
      # Run the stored procedure with the current args on the database
      def run(&block)
        case @sproc_type
        when :select, :all
          all(&block)
        when :first
          first
        when :insert
          insert
        when :update
          update
        when :delete
          delete
        end
      end
      
      # Set the type of the stored procedure and override the corresponding _sql
      # method to return the empty string (since the result will be
      # ignored anyway).
      def sproc_type=(type)
        @sproc_type = type
        @opts[:sql] = ''
      end
    end
  
    module StoredProcedures
      # For the given type (:select, :first, :insert, :update, or :delete),
      # run the database stored procedure with the given name with the given
      # arguments.
      def call_sproc(type, name, *args)
        prepare_sproc(type, name).call(*args)
      end
      
      # Transform this dataset into a stored procedure that you can call
      # multiple times with new arguments.
      def prepare_sproc(type, name)
        sp = clone
        prepare_extend_sproc(sp)
        sp.sproc_type = type
        sp.sproc_name = name
        sp
      end
      
      private
      
      # Extend the dataset with the stored procedure methods.
      def prepare_extend_sproc(ds)
        ds.extend(StoredProcedureMethods)
      end
    end
  end
end
