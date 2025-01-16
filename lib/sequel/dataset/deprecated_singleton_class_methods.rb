# frozen-string-literal: true

module Sequel
  class Dataset
    # This module implements methods to support deprecated use of extensions registered
    # not using a module.  In such cases, for backwards compatibility, Sequel has to use
    # a singleton class for the dataset.
    module DeprecatedSingletonClassMethods
      # Load the extension into a clone of the receiver.
      def extension(*a)
        c = _clone(:freeze=>false)
        c.send(:_extension!, a)
        c.freeze
      end

      # Extend the cloned of the receiver with the given modules, instead of the default
      # approach of creating a subclass of the receiver's class and including the modules
      # into that.
      def with_extend(*mods, &block)
        c = _clone(:freeze=>false)
        c.extend(*mods) unless mods.empty?
        c.extend(Sequel.set_temp_name(DatasetModule.new(&block)){"Sequel::Dataset::_DatasetModule(#{block.source_location.join(':')})"}) if block
        c.freeze
      end

      private

      # Load the extensions into the receiver.
      def _extension!(exts)
        Sequel.extension(*exts)
        exts.each do |ext|
          if pr = Sequel.synchronize{EXTENSIONS[ext]}
            pr.call(self)
          else
            raise(Error, "Extension #{ext} does not have specific support handling individual datasets (try: Sequel.extension #{ext.inspect})")
          end
        end
        self
      end
    end
  end
end
