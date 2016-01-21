# frozen-string-literal: true

module Sequel
  module Plugins
    # The prepared_statements_associations plugin modifies the regular association
    # load method to use a cached prepared statement to load the associations.
    # It will not work on all associations, but it should skip the use of prepared
    # statements for associations where it will not work, assuming you load the
    # plugin before defining the associations.
    #
    # Usage:
    #
    #   # Make all model subclasses more safe when using prepared statements (called before loading subclasses)
    #   Sequel::Model.plugin :prepared_statements_associations
    #
    #   # Make the Album class more safe when using prepared statements
    #   Album.plugin :prepared_statements_associations
    module PreparedStatementsAssociations
      # Synchronize access to the integer sequence so that no two calls get the same integer.
      MUTEX = Mutex.new

      i = 0
      # This plugin names prepared statements uniquely using an integer sequence, this
      # lambda returns the next integer to use.
      NEXT = lambda{MUTEX.synchronize{i += 1}}

      module InstanceMethods
        private

        # Return a bound variable hash that maps the keys in +ks+ (qualified by the +table+)
        # to the values of the results of sending the methods in +vs+.
        def association_bound_variable_hash(table, ks, vs)
          Hash[*ks.zip(vs).map{|k, v| [:"#{table}.#{k}", get_column_value(v)]}.flatten]
        end

        # Given an association reflection, return a bound variable hash for the given
        # association for this instance's values.
        def association_bound_variables(opts)
          case opts[:type]
          when :many_to_one
            association_bound_variable_hash(opts.associated_class.table_name, opts.primary_keys, opts[:keys])
          when :one_to_many, :one_to_one
            association_bound_variable_hash(opts.associated_class.table_name, opts[:keys], opts[:primary_keys])
          when :many_to_many, :one_through_one
            association_bound_variable_hash(opts.join_table_alias, opts[:left_keys], opts[:left_primary_keys])
          when :many_through_many, :one_through_many
            association_bound_variable_hash(opts.final_reverse_edge[:alias], Array(opts[:left_key]), opts[:left_primary_keys])
          end
        end

        # Given an association reflection, return and cache a prepared statement for this association such
        # that, given appropriate bound variables, the prepared statement will work correctly for any
        # instance.  Return false if such a prepared statement cannot be created.
        def association_prepared_statement(opts, assoc_bv)
          return unless model.cache_associations
          opts.send(:cached_fetch, :prepared_statement) do
            unless opts[:instance_specific]
              ds, bv = _associated_dataset(opts, {}).unbind

              f = ds.opts[:from]
              if f && f.length == 1
                s = ds.opts[:select]
                if ds.opts[:join]
                  if opts.eager_loading_use_associated_key? && s && s.length == 1 && s.first.is_a?(SQL::ColumnAll)
                    table = s.first.table
                    ds = ds.select(*opts.associated_class.columns.map{|c| Sequel.identifier(c).qualify(table)})
                  end
                elsif !s || s.empty?
                  ds = ds.select(*opts.associated_class.columns.map{|c| Sequel.identifier(c)})
                end
              end 
          
              if bv.length != assoc_bv.length
                h = {}
                bv.each do |k,v|
                  h[k] = v unless assoc_bv.has_key?(k)
                end
                ds = ds.bind(h)
              end
              ps = ds.prepare(opts.returns_array? ? :select : :first, :"smpsap_#{NEXT.call}")
              ps.log_sql = true
              ps
            end
          end
        end

        # Use a prepared statement if possible to load the associated object,
        # unless a dynamic callback is given.
        def _load_associated_object(opts, dynamic_opts)
          if !dynamic_opts[:callback] && (bv = association_bound_variables(opts)) && (ps ||= association_prepared_statement(opts, bv))
            ps.call(bv)
          else
            super
          end
        end

        # Use a prepared statement if possible to load the associated object,
        # unless the associated model uses caching.
        def _load_associated_object_via_primary_key(opts)
          if !opts.associated_class.respond_to?(:cache_get_pk) && (bv = association_bound_variables(opts)) && (ps ||= association_prepared_statement(opts, bv))
            ps.call(bv)
          else
            super
          end
        end

        # Use a prepared statement if possible to load the associated objects,
        # unless a dynamic callback is given.
        def _load_associated_object_array(opts, dynamic_opts)
          if !dynamic_opts[:callback] && (bv = association_bound_variables(opts)) && (ps ||= association_prepared_statement(opts, bv))
            ps.call(bv)
          else
            super
          end
        end
      end
    end
  end
end
