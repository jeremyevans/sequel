module Sequel
  module Plugins
    # The association_pks plugin adds the association_pks and association_pks=
    # instance methods to the model class for each association added.  These
    # methods allow for easily returning the primary keys of the associated
    # objects, and easily modifying the associated objects to set the primary
    # keys to just the ones given:
    #
    #   Artist.one_to_many :albums
    #   artist = Artist[1]
    #   artist.album_pks # [1, 2, 3]
    #   artist.album_pks = [2, 4]
    #   artist.album_pks # [2, 4]
    #
    # Note that it uses the singular form of the association name. Also note
    # that the setter both associates to new primary keys not in the assocation
    # and disassociates from primary keys not provided to the method.
    #
    # This plugin makes modifications directly to the underlying tables,
    # it does not create or return any model objects, and therefore does
    # not call any callbacks.  If you have any association callbacks,
    # you probably should not use the setter methods.
    #
    # This plugin only works with singular primary keys, it does not work
    # with composite primary keys.
    # 
    # Usage:
    #
    #   # Make all model subclass *_to_many associations have association_pks
    #   # methods (called before loading subclasses)
    #   Sequel::Model.plugin :association_pks
    #
    #   # Make the Album *_to_many associations have association_pks
    #   # methods (called before the association methods)
    #   Album.plugin :association_pks
    module AssociationPks
      module ClassMethods
        private

        # Define a association_pks method using the block for the association reflection 
        def def_association_pks_getter(opts, &block)
          association_module_def(:"#{singularize(opts[:name])}_pks", opts, &block)
        end

        # Define a association_pks= method using the block for the association reflection,
        # if the association is not read only.
        def def_association_pks_setter(opts, &block)
          association_module_def(:"#{singularize(opts[:name])}_pks=", opts, &block) unless opts[:read_only]
        end

        # Add a getter that checks the join table for matching records and
        # a setter that deletes from or inserts into the join table.
        def def_many_to_many(opts)
          super
          def_association_pks_getter(opts) do
            _join_table_dataset(opts).filter(opts[:left_key]=>send(opts[:left_primary_key])).select_map(opts[:right_key])
          end
          def_association_pks_setter(opts) do |pks|
            pks = convert_pk_array(opts, pks)
            checked_transaction do
              ds = _join_table_dataset(opts).filter(opts[:left_key]=>send(opts[:left_primary_key]))
              ds.exclude(opts[:right_key]=>pks).delete
              pks -= ds.select_map(opts[:right_key])
              pks.each do |pk|
                insert = Hash[Array(opts[:right_key]).zip(Array(pk))]
                insert[opts[:left_key]] = send(opts[:left_primary_key])
                ds.insert(insert)
              end
            end
          end
        end

        # Add a getter that checks the association dataset and a setter
        # that updates the associated table.
        def def_one_to_many(opts)
          super
          return if opts[:type] == :one_to_one
          def_association_pks_getter(opts) do
            send(opts.dataset_method).select_map(opts.associated_class.primary_key)
          end
          def_association_pks_setter(opts) do |pks|
            pks = convert_pk_array(opts, pks)
            checked_transaction do
              ds = send(opts.dataset_method)
              primary_key = opts.associated_class.primary_key
              key = opts[:key]
              ds.unfiltered.filter(primary_key=>pks).update(key=>pk)
              ds.exclude(primary_key=>pks).update(key=>nil)
            end
          end
        end
      end

      module InstanceMethods
        private

        # If the associated class's primary key column type is integer,
        # typecast all provided values to integer before using them.
        def convert_pk_array(opts, pks)
          if klass = opts.associated_class and sch = klass.db_schema and col = sch[klass.primary_key] and col[:type] == :integer
            pks.map{|pk| model.db.typecast_value(:integer, pk)}
          else
            pks
          end
        end
      end
    end
  end
end
