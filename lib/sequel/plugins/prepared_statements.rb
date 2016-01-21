# frozen-string-literal: true

module Sequel
  class Model
    module InstanceMethods
      # Whether prepared statements should be used for the given type of query
      # (:insert, :insert_select, :refresh, :update, or :delete).  True by default,
      # can be overridden in other plugins to disallow prepared statements for
      # specific types of queries.
      def use_prepared_statements_for?(type)
        true
      end
    end
  end

  module Plugins
    # The prepared_statements plugin modifies the model to use prepared statements for
    # instance level deletes and saves, as well as class level lookups by
    # primary key.
    #
    # Note that this plugin is unsafe in some circumstances, as it can allow up to
    # 2^N prepared statements to be created for each type of insert and update query, where
    # N is the number of colums in the table. It is recommended that you use the
    # +prepared_statements_safe+ plugin in addition to this plugin to reduce the number
    # of prepared statements that can be created, unless you tightly control how your
    # model instances are saved.
    # 
    # Usage:
    #
    #   # Make all model subclasses use prepared statements  (called before loading subclasses)
    #   Sequel::Model.plugin :prepared_statements
    #
    #   # Make the Album class use prepared statements
    #   Album.plugin :prepared_statements
    module PreparedStatements
      # Synchronize access to the integer sequence so that no two calls get the same integer.
      MUTEX = Mutex.new
      
      i = 0
      # This plugin names prepared statements uniquely using an integer sequence, this
      # lambda returns the next integer to use.
      NEXT = lambda{MUTEX.synchronize{i += 1}}

      # Setup the datastructure used to hold the prepared statements in the model.
      def self.apply(model)
        model.instance_variable_set(:@prepared_statements, :insert=>{}, :insert_select=>{}, :update=>{}, :lookup_sql=>{}, :fixed=>{})
      end

      module ClassMethods
        Plugins.inherited_instance_variables(self, :@prepared_statements=>lambda{|v| {:insert=>{}, :insert_select=>{}, :update=>{}, :lookup_sql=>{}, :fixed=>{}}})

        private

        # Create a prepared statement, but modify the SQL used so that the model's columns are explicitly
        # selected instead of using *, assuming that the dataset selects from a single table.
        def prepare_explicit_statement(ds, type, vals=OPTS)
          f = ds.opts[:from]
          meth = type == :insert_select ? :returning : :select
          s = ds.opts[meth]
          if f && f.length == 1 && !ds.opts[:join] && (!s || s.empty?)
            ds = ds.send(meth, *columns.map{|c| Sequel.identifier(c)})
          end 
          
          prepare_statement(ds, type, vals)
        end

        # Create a prepared statement based on the given dataset with a unique name for the given
        # type of query and values.
        def prepare_statement(ds, type, vals=OPTS)
          ps = ds.prepare(type, :"smpsp_#{NEXT.call}", vals)
          ps.log_sql = true
          ps
        end

        # Return a sorted array of columns for use as a hash key.
        def prepared_columns(cols)
          RUBY_VERSION >= '1.9' ? cols.sort : cols.sort_by(&:to_s)
        end

        # Return a prepared statement that can be used to delete a row from this model's dataset.
        def prepared_delete
          cached_prepared_statement(:fixed, :delete){prepare_statement(filter(prepared_statement_key_array(primary_key)), :delete)}
        end

        # Return a prepared statement that can be used to insert a row using the given columns.
        def prepared_insert(cols)
          cached_prepared_statement(:insert, prepared_columns(cols)){prepare_statement(dataset, :insert, prepared_statement_key_hash(cols))}
        end

        # Return a prepared statement that can be used to insert a row using the given columns
        # and return that column values for the row created.
        def prepared_insert_select(cols)
          if dataset.supports_insert_select?
            cached_prepared_statement(:insert_select, prepared_columns(cols)){prepare_explicit_statement(naked.clone(:server=>dataset.opts.fetch(:server, :default)), :insert_select, prepared_statement_key_hash(cols))}
          end
        end

        # Return a prepared statement that can be used to lookup a row solely based on the primary key.
        def prepared_lookup
          cached_prepared_statement(:fixed, :lookup){prepare_explicit_statement(filter(prepared_statement_key_array(primary_key)), :first)}
        end

        # Return a prepared statement that can be used to refresh a row to get new column values after insertion.
        def prepared_refresh
          cached_prepared_statement(:fixed, :refresh){prepare_explicit_statement(naked.clone(:server=>dataset.opts.fetch(:server, :default)).filter(prepared_statement_key_array(primary_key)), :first)}
        end

        # Return an array of two element arrays with the column symbol as the first entry and the
        # placeholder symbol as the second entry.
        def prepared_statement_key_array(keys)
          if dataset.requires_placeholder_type_specifiers?
            sch = db_schema
            Array(keys).map do |k|
              if (s = sch[k]) && (t = s[:type])
                [k, :"$#{k}__#{t}"]
              else
                [k, :"$#{k}"]
              end
            end
          else
            Array(keys).map{|k| [k, :"$#{k}"]}
          end
        end

        # Return a hash mapping column symbols to placeholder symbols.
        def prepared_statement_key_hash(keys)
          Hash[*(prepared_statement_key_array(keys).flatten)]
        end

        # Return a prepared statement that can be used to update row using the given columns.
        def prepared_update(cols)
          cached_prepared_statement(:update, prepared_columns(cols)){prepare_statement(filter(prepared_statement_key_array(primary_key)), :update, prepared_statement_key_hash(cols))}
        end

        # Use a prepared statement to query the database for the row matching the given primary key.
        def primary_key_lookup(pk)
          prepared_lookup.call(primary_key_hash(pk))
        end

        # If a prepared statement has already been cached for the given type and subtype,
        # return it.  Otherwise, yield to the block to get the prepared statement, and cache it.
        def cached_prepared_statement(type, subtype)
          h = @prepared_statements[type]
          Sequel.synchronize do
            if v = h[subtype]
              return v
            end
          end
          ps = yield
          Sequel.synchronize{h[subtype] = ps}
        end
      end

      module InstanceMethods
        private

        # Use a prepared statement to delete the row.
        def _delete_without_checking
          if use_prepared_statements_for?(:delete)
            model.send(:prepared_delete).call(pk_hash)
          else
            super
          end
        end

        # Use a prepared statement to insert the values into the model's dataset.
        def _insert_raw(ds)
          if use_prepared_statements_for?(:insert)
            model.send(:prepared_insert, @values.keys).call(@values)
          else
            super
          end
        end

        # Use a prepared statement to insert the values into the model's dataset
        # and return the new column values.
        def _insert_select_raw(ds)
          if use_prepared_statements_for?(:insert_select)
            if ps = model.send(:prepared_insert_select, @values.keys)
              ps.call(@values)
            end
          else
            super
          end
        end

        # Use a prepared statement to refresh this model's column values.
        def _refresh_get(ds)
          if use_prepared_statements_for?(:refresh)
            model.send(:prepared_refresh).call(pk_hash)
          else
            super
          end
        end

        # Use a prepared statement to update this model's columns in the database.
        def _update_without_checking(columns)
          if use_prepared_statements_for?(:update)
            model.send(:prepared_update, columns.keys).call(Hash[columns].merge!(pk_hash))
          else
            super
          end
        end
      end
    end
  end
end
