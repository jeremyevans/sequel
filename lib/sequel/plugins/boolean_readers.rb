module Sequel
  module Plugins
    # The BooleaReaders plugin allows for the creation of attribute? methods
    # for boolean columns, which provide a nicer API.  By default, the accessors
    # are created for all columns of type :boolean.  However, you can provide a
    # block to the plugin to change the criteria used to determine if a
    # column is boolean:
    #
    #   Sequel::Model.plugin(:boolean_readers){|c| db_schema[c][:db_type] =~ /\Atinyint/}
    #
    # This may be useful if you are using MySQL and have some tinyint columns
    # that represent booleans and others that represent integers.  You can turn
    # the convert_tinyint_to_bool setting off and use the attribute methods for
    # the integer value and the attribute? methods for the boolean value.
    module BooleanReaders
      # Default proc for determining if given column is a boolean, which
      # just checks that the :type is boolean.
      DEFAULT_BOOLEAN_ATTRIBUTE_PROC = lambda{|c| s = db_schema[c] and s[:type] == :boolean}

      # Add the boolean_attribute? class method to the model, and create
      # attribute? boolean reader methods for the class's columns if the class has a dataset.
      def self.configure(model, &block)
        model.meta_def(:boolean_attribute?, &(block || DEFAULT_BOOLEAN_ATTRIBUTE_PROC))
        model.instance_eval{send(:create_boolean_readers) if @dataset}
      end

      module ClassMethods
        # Create boolean readers for the class using the columns from the new dataset.
        def set_dataset(*args)
          super
          create_boolean_readers
          self
        end

        private

        # Add a attribute? method for the column to a module included in the class.
        def create_boolean_reader(column)
          overridable_methods_module.module_eval do
            define_method("#{column}?"){model.db.typecast_value(:boolean, send(column))}
          end
        end

        # Add attribute? methods for all of the boolean attributes for this model.
        def create_boolean_readers
          im = instance_methods.collect{|x| x.to_s}
          cs = columns rescue return
          cs.each{|c| create_boolean_reader(c) if boolean_attribute?(c) && !im.include?("#{c}?")}
        end
      end
    end
  end
end
