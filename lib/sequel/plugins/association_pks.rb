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
          # Grab values from the reflection so that the hash lookup only needs to be
          # done once instead of inside ever method call.
          lk, lpk, rk = opts.values_at(:left_key, :left_primary_key, :right_key)

          # Add 2 separate implementations of the getter method optimized for the
          # composite and singular left key cases, and 4 separate implementations of the setter
          # method optimized for each combination of composite and singular keys for both
          # the left and right keys.
          if lpk.is_a?(Array)
            def_association_pks_getter(opts) do
              h = {}
              lk.zip(lpk).each{|k, pk| h[k] = send(pk)}
              _join_table_dataset(opts).filter(h).select_map(rk)
            end

            if rk.is_a?(Array)
              def_association_pks_setter(opts) do |pks|
                pks = convert_cpk_array(opts, pks)
                checked_transaction do
                  lpkv = lpk.map{|k| send(k)}
                  ds = _join_table_dataset(opts).filter(lk.zip(lpkv))
                  ds.exclude(rk=>pks).delete
                  pks -= ds.select_map(rk)
                  h = {}
                  lk.zip(lpkv).each{|k, v| h[k] = v}
                  pks.each do |pk|
                    ih = h.dup
                    rk.zip(pk).each{|k, v| ih[k] = v}
                    ds.insert(ih)
                  end
                end
              end
            else
              def_association_pks_setter(opts) do |pks|
                pks = convert_pk_array(opts, pks)
                checked_transaction do
                  lpkv = lpk.map{|k| send(k)}
                  ds = _join_table_dataset(opts).filter(lk.zip(lpkv))
                  ds.exclude(rk=>pks).delete
                  pks -= ds.select_map(rk)
                  h = {}
                  lk.zip(lpkv).each{|k, v| h[k] = v}
                  pks.each do |pk|
                    ds.insert(h.merge(rk=>pk))
                  end
                end
              end
            end
          else
            def_association_pks_getter(opts) do
              _join_table_dataset(opts).filter(lk=>send(lpk)).select_map(rk)
            end

            if rk.is_a?(Array)
              def_association_pks_setter(opts) do |pks|
                pks = convert_cpk_array(opts, pks)
                checked_transaction do
                  lpkv = send(lpk)
                  ds = _join_table_dataset(opts).filter(lk=>lpkv)
                  ds.exclude(rk=>pks).delete
                  pks -= ds.select_map(rk)
                  pks.each do |pk|
                    h = {lk=>lpkv}
                    rk.zip(pk).each{|k, v| h[k] = v}
                    ds.insert(h)
                  end
                end
              end
            else
              def_association_pks_setter(opts) do |pks|
                pks = convert_pk_array(opts, pks)
                checked_transaction do
                  lpkv = send(lpk)
                  ds = _join_table_dataset(opts).filter(lk=>lpkv)
                  ds.exclude(rk=>pks).delete
                  pks -= ds.select_map(rk)
                  pks.each{|pk| ds.insert(lk=>lpkv, rk=>pk)}
                end
              end
            end
          end
        end

        # Add a getter that checks the association dataset and a setter
        # that updates the associated table.
        def def_one_to_many(opts)
          super
          return if opts[:type] == :one_to_one

          key = opts[:key]

          def_association_pks_getter(opts) do
            send(opts.dataset_method).select_map(opts.associated_class.primary_key)
          end

          def_association_pks_setter(opts) do |pks|
            primary_key = opts.associated_class.primary_key

            pks = if primary_key.is_a?(Array)
              convert_cpk_array(opts, pks)
            else
              convert_pk_array(opts, pks)
            end

            pkh = {primary_key=>pks}

            if key.is_a?(Array)
              h = {}
              nh = {}
              key.zip(pk).each do|k, v|
                h[k] = v
                nh[k] = nil
              end
            else
              h = {key=>pk}
              nh = {key=>nil}
            end

            checked_transaction do
              ds = send(opts.dataset_method)
              ds.unfiltered.filter(pkh).update(h)
              ds.exclude(pkh).update(nh)
            end
          end
        end
      end

      module InstanceMethods
        private

        # If any of associated class's composite primary key column types is integer,
        # typecast the appropriate values to integer before using them.
        def convert_cpk_array(opts, cpks)
          if klass = opts.associated_class and sch = klass.db_schema and (cols = sch.values_at(*klass.primary_key)).all? and (convs = cols.map{|c| c[:type] == :integer}).any?
            cpks.map do |cpk|
              cpk.zip(convs).map do |pk, conv|
                conv ? model.db.typecast_value(:integer, pk) : pk
              end
            end
          else
            cpks
          end
        end

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
