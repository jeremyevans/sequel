# frozen-string-literal: true

module Sequel
  module Plugins
    # The association_pks plugin adds association_pks and association_pks=
    # instance methods to the model class for each association added.  These
    # methods allow for easily returning the primary keys of the associated
    # objects, and easily modifying which objects are associated:
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
    # If an association uses the :delay_pks option, you can set the associated
    # pks for new objects, and the setting will not be persisted until after the
    # object has been created in the database.  Additionally, if an association
    # uses the :delay_pks=>:always option, you can set the associated pks for existing
    # objects, and the setting will not be persisted until after the object has
    # been saved.
    #
    # By default, if you pass a nil value to the setter, an exception will be raised.
    # You can change this behavior by using the :association_pks_nil association option.
    # If set to :ignore, the setter will take no action if nil is given.
    # If set to :remove, the setter will treat the nil as an empty array, removing
    # the association all currently associated values.
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
        def def_association_pks_methods(opts)
          association_module_def(:"#{singularize(opts[:name])}_pks", opts){_association_pks_getter(opts)}
          association_module_def(:"#{singularize(opts[:name])}_pks=", opts){|pks| _association_pks_setter(opts, pks)} unless opts[:read_only]
        end

        # Add a getter that checks the join table for matching records and
        # a setter that deletes from or inserts into the join table.
        def def_many_to_many(opts)
          super

          return if opts[:type] == :one_through_one

          # Grab values from the reflection so that the hash lookup only needs to be
          # done once instead of inside every method call.
          lk, lpk, rk = opts.values_at(:left_key, :left_primary_key, :right_key)
          clpk = lpk.is_a?(Array)
          crk = rk.is_a?(Array)

          opts[:pks_getter] = if clpk
            lambda do
              h = {}
              lk.zip(lpk).each{|k, pk| h[k] = get_column_value(pk)}
              _join_table_dataset(opts).where(h).select_map(rk)
            end
          else
            lambda do
              _join_table_dataset(opts).where(lk=>get_column_value(lpk)).select_map(rk)
            end
          end

          opts[:pks_setter] = lambda do |pks|
            if pks.empty?
              send(opts.remove_all_method)
            else
              checked_transaction do
                if clpk
                  lpkv = lpk.map{|k| get_column_value(k)}
                  cond = lk.zip(lpkv)
                else
                  lpkv = get_column_value(lpk)
                  cond = {lk=>lpkv}
                end
                ds = _join_table_dataset(opts).where(cond)
                ds.exclude(rk=>pks).delete
                pks -= ds.select_map(rk)
                lpkv = Array(lpkv)
                key_array = crk ? pks.map{|pk| lpkv + pk} : pks.map{|pk| lpkv + [pk]}
                key_columns = Array(lk) + Array(rk)
                ds.import(key_columns, key_array)
              end
            end
          end

          def_association_pks_methods(opts)
        end

        # Add a getter that checks the association dataset and a setter
        # that updates the associated table.
        def def_one_to_many(opts)
          super

          return if opts[:type] == :one_to_one

          key = opts[:key]

          opts[:pks_getter] = lambda do
            send(opts.dataset_method).select_map(opts.associated_class.primary_key)
          end

          opts[:pks_setter] = lambda do |pks|
            if pks.empty?
              send(opts.remove_all_method)
            else
              primary_key = opts.associated_class.primary_key
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
                ds.unfiltered.where(pkh).update(h)
                ds.exclude(pkh).update(nh)
              end
            end
          end

          def_association_pks_methods(opts)
        end
      end

      module InstanceMethods
        # After creating an object, if there are any saved association pks,
        # call the related association pks setters.
        def after_save
          if assoc_pks = @_association_pks
            assoc_pks.each do |name, pks|
              instance_exec(pks, &model.association_reflection(name)[:pks_setter])
            end
            @_association_pks = nil
          end
          super
        end

        # Clear the associated pks if explicitly refreshing.
        def refresh
          @_association_pks = nil
          super
        end

        private

        # Return the primary keys of the associated objects.
        # If the receiver is a new object, return any saved
        # pks, or an empty array if no pks have been saved.
        def _association_pks_getter(opts)
          delay = opts[:delay_pks]
          if new? && delay
            (@_association_pks ||= {})[opts[:name]] ||= []
          elsif delay == :always && @_association_pks && (objs = @_association_pks[opts[:name]])
            objs
          else
            instance_exec(&opts[:pks_getter])
          end
        end

        # Update which objects are associated to the receiver.
        # If the receiver is a new object, save the pks
        # so the update can happen after the received has been saved.
        def _association_pks_setter(opts, pks)
          if pks.nil?
            case opts[:association_pks_nil]
            when :remove
              pks = []
            when :ignore
              return
            else
              raise Error, "nil value given to association_pks setter"
            end
          end

          pks = convert_pk_array(opts, pks)

          delay = opts[:delay_pks]
          if (new? && delay) || (delay == :always)
            modified!
            (@_association_pks ||= {})[opts[:name]] = pks
          else
            instance_exec(pks, &opts[:pks_setter])
          end
        end

        # If the associated class's primary key column type is integer,
        # typecast all provided values to integer before using them.
        def convert_pk_array(opts, pks)
          klass = opts.associated_class
          primary_key = klass.primary_key
          sch = klass.db_schema

          if primary_key.is_a?(Array)
            if (cols = sch.values_at(*klass.primary_key)).all? && (convs = cols.map{|c| c[:type] == :integer}).all?
              pks.map do |cpk|
                cpk.zip(convs).map do |pk, conv|
                  conv ? model.db.typecast_value(:integer, pk) : pk
                end
              end
            else
              pks
            end
          elsif (col = sch[klass.primary_key]) && (col[:type] == :integer)
            pks.map{|pk| model.db.typecast_value(:integer, pk)}
          else
            pks
          end
        end
      end
    end
  end
end
