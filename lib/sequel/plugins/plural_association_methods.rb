# frozen-string-literal: true

module Sequel
  module Plugins
    module PluralAssociationMethods
      # The `plural_association_methods` plugin adds `Sequel::Model#add_*s` and
      # `Sequel::Model#remove_*s` methods for handy batch operations. Example:
      #
      #   artist.albums # => [album1]
      #   artist.add_albums([album2, album3])
      #   artist.albums # => [album1, album2, album3]
      #   artist.remove_albums([album3, album1])
      #   artist.albums # => [album2]
      #
      # It can handle situations with prymary keys, composite keys, redefined adders and removers,
      # additional arguments, etc. - everything like with singular methods.
      #
      # It also saves the reciprocal's state:
      #
      #   artist.remove_albums([album3, album1])
      #   album1.artist # => nil
      #   album3.artist # => nil
      #
      # There is one caveat; it runs with separate queries for adder and remover logic preserve.
      #
      # Usage:
      #
      #   # Add plural association methods for all model subclass instances
      #   # (called before associations defining)
      #   Sequel::Model.plugin :plural_association_methods
      #
      #   # Add plural association methods for the `Artist` class
      #   Album.plugin :plural_association_methods
      module ClassMethods
        # Define the plural association instance methods.
        def def_association_instance_methods(opts)
          super

          if opts[:adder]
            association_module_def(:"add_#{opts[:name]}", opts) do |objs, *args|
              objs.map { |obj| send(opts[:add_method], obj, *args) }.compact
            end
          end

          if opts[:remover]
            association_module_def(:"remove_#{opts[:name]}", opts) do |objs, *args|
              objs.map { |obj| send(opts[:remove_method], obj, *args) }.compact
            end
          end
        end
      end
    end
  end
end