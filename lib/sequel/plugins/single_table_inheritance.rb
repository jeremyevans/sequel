module Sequel
  module Plugins
    # Sequel's built in Single Table Inheritance plugin makes subclasses
    # of this model only load rows where the given key field matches the
    # subclass's name.  If the key given has a NULL value or there are
    # any problems looking up the class, uses the current class.
    #   
    # You should only use this in the parent class, not in the subclasses.
    #   
    # You shouldn't call set_dataset in the model after applying this
    # plugin, otherwise subclasses might use the wrong dataset.
    #   
    # The filters and row_proc that sti_key sets up in subclasses may not work correctly if
    # those subclasses have further subclasses.  For those middle subclasses,
    # you may need to call set_dataset manually with the correct filter and
    # row_proc.
    module SingleTableInheritance
      # Set the sti_key and sti_dataset for the model, and change the
      # dataset's row_proc so that the dataset yields objects of varying classes,
      # where the class used has the same name as the key field.
      def self.configure(model, key)
        m = model.method(:constantize)
        model.instance_eval do
          @sti_key = key 
          @sti_dataset = dataset
          dataset.row_proc = lambda{|r| (m.call(r[key]) rescue model).load(r)}
        end
      end

      module ClassMethods
        # The base dataset for STI, to which filters are added to get
        # only the models for the specific STI subclass.
        attr_reader :sti_dataset

        # The column name holding the STI key for this model
        attr_reader :sti_key

        # Copy the sti_key and sti_dataset to the subclasses, and filter the
        # subclass's dataset so it is restricted to rows where the key column
        # matches the subclass's name.
        def inherited(subclass)
          super
          sk = sti_key
          sd = sti_dataset
          subclass.set_dataset(sd.filter(SQL::QualifiedIdentifier.new(table_name, sk)=>subclass.name.to_s), :inherited=>true)
          subclass.instance_eval do
            @sti_key = sk
            @sti_dataset = sd
            @simple_table = nil
          end
        end
      end

      module InstanceMethods
        # Set the sti_key column to the name of the model.
        def before_create
          return false if super == false
          send("#{model.sti_key}=", model.name.to_s) unless send(model.sti_key)
        end
      end
    end
  end
end
