module Sequel
  module Plugins
    # The lazy_attributes plugin allows users to easily set that some attributes
    # should not be loaded by default when loading model objects.  If the attribute
    # is needed after the instance has been retreived, a database query is made to
    # retreive the value of the attribute.
    #
    # This plugin depends on the tactical_eager_loading plugin, and allows you to
    # eagerly load lazy attributes for all objects retreived with the current object.
    # So the following code should issue one query to get the albums and one query to
    # get the reviews for all of those albums:
    #
    #   Album.plugin :lazy_attributes
    #   Album.lazy_attributes :review
    #   Sequel::Model.with_identity_map do
    #     Album.filter{id<100}.all do |a|
    #       a.review
    #     end
    #   end
    module LazyAttributes
      module ClassMethods
        # Remove the given attributes from the list of columns selected by default.
        # For each attribute given, create an accessor method that allows a lazy
        # lookup of the attribute.  Each attribute should be given as a symbol.
        def lazy_attributes(*attrs)
          set_dataset(dataset.select(*(columns - attrs)))
          attrs.each do |a|
            define_method(a) do
              if !values.include?(a) && !@new
                lazy_attribute_lookup(a)
              else
                super
              end
            end
          end
        end
      end

      module InstanceMethods
        private

        # If the model was selected with other model objects, eagerly load the
        # attribute for all of those objects.  If not, query the database for
        # the attribute for just the current object.  Return the value of
        # the attribute for the current object.
        def lazy_attribute_lookup(a)
          primary_key = model.primary_key
          model.select(*(Array(primary_key) + [a])).filter(primary_key=>retreived_with.map{|o| o.pk}.sql_array).all if model.identity_map && retreived_with
          values[a] = model.select(a).first(primary_key=>pk)[a] unless values.include?(a)
          values[a]
        end
      end
    end
  end
end
