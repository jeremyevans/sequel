# frozen-string-literal: true

module Sequel
  module Plugins
    # The subset_conditions plugin creates an additional *_conditions method
    # for every `subset`, `where`, and `exclude` method call in a dataset_module
    # block. This method returns the filter conditions, which can be useful if
    # you want to use the conditions for a separate filter or combine them with OR.
    # It also supports where_all and where_any dataset_module methods for
    # combining multiple dataset method filters with AND or OR.
    #
    # Usage:
    #
    #   # Add subset_conditions in the Album class
    #   Album.plugin :subset_conditions
    #
    #   Album.dataset_module do
    #     # This will now create a published_conditions method
    #     where :published, published: true
    #
    #     # This will now create a not_bad_conditions method
    #     exclude :not_bad, :bad
    #
    #     # This will create good_and_available and
    #     # good_and_available_conditions methods
    #     where_all :good_and_available, :published, :not_bad
    #
    #     # This will create good_or_available and
    #     # good_or_available_conditions methods
    #     where_any :good_or_available, :published, :not_bad
    #   end
    #
    #   Album.where(Album.published_conditions).sql
    #   # SELECT * FROM albums WHERE (published IS TRUE)
    #
    #   Album.exclude(Album.published_conditions).sql
    #   # SELECT * FROM albums WHERE (published IS NOT TRUE)
    #
    #   Album.where(Album.published_conditions | {ready: true}).sql
    #   # SELECT * FROM albums WHERE ((published IS TRUE) OR (ready IS TRUE))
    #
    #   Album.good_and_available.sql
    #   SELECT * FROM albums WHERE ((published IS TRUE) AND NOT bad)
    #
    #   Album.good_or_available.sql
    #   SELECT * FROM albums WHERE ((published IS TRUE) OR NOT bad)
    module SubsetConditions
      def self.apply(model, &block)
        model.instance_exec do
          @dataset_module_class = Class.new(@dataset_module_class) do
            Sequel.set_temp_name(self){"#{model.name}::@dataset_module_class(SubsetConditions)"}
            include DatasetModuleMethods
          end
        end
      end

      module DatasetModuleMethods
        # Also create a method that returns the conditions the filter uses.
        def where(name, *args, &block)
          super
          cond = args
          cond = cond.first if cond.size == 1
          define_method(:"#{name}_conditions"){filter_expr(cond, &block)}
        end

        # Also create a method that returns the conditions the filter uses.
        def exclude(name, *args, &block)
          super
          cond = args
          cond = cond.first if cond.size == 1
          define_method(:"#{name}_conditions"){Sequel.~(filter_expr(cond, &block))}
        end

        # Create a method that combines filters from already registered
        # dataset methods, and filters for rows where all of the conditions
        # are satisfied.
        #
        #   Employee.dataset_module do
        #     where :active, active: true
        #     where :started, Sequel::CURRENT_DATE <= :start_date
        #     where_all(:active_and_started, :active, :started)
        #   end
        #
        #   Employee.active_and_started.sql
        #   # SELECT * FROM employees WHERE ((active IS TRUE) AND (CURRENT_DATE <= start_date))
        def where_all(name, *args)
          _where_any_all(:&, name, args)
        end

        # Create a method that combines filters from already registered
        # dataset methods, and filters for rows where any of the conditions
        # are satisfied.
        #
        #   Employee.dataset_module do
        #     where :active, active: true
        #     where :started, Sequel::CURRENT_DATE <= :start_date
        #     where_any(:active_or_started, :active, :started)
        #   end
        #
        #   Employee.active_or_started.sql
        #   # SELECT * FROM employees WHERE ((active IS TRUE) OR (CURRENT_DATE <= start_date))
        def where_any(name, *args)
          _where_any_all(:|, name, args)
        end

        private

        if RUBY_VERSION >= '2'
          # Backbone of #where_any and #where_all
          def _where_any_all(meth, name, args)
            ds = model.dataset
            # #bind used here because the dataset module may not yet be included in the model's dataset
            where(name, Sequel.send(meth, *args.map{|a| self.instance_method(:"#{a}_conditions").bind(ds).call}))
          end
        else
          # Cannot bind module method to arbitrary objects in Ruby 1.9.
          # :nocov:
          def _where_any_all(meth, name, args)
            ds = model.dataset.clone
            ds.extend(self)
            where(name, Sequel.send(meth, *args.map{|a| ds.send(:"#{a}_conditions")}))
          end
          # :nocov:
        end
      end
    end
  end
end
