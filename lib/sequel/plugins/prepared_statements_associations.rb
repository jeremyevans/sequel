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

      module ClassMethods
        # Disable prepared statement use if a block is given, or the :dataset or :conditions
        # options are used, or you are cloning an association.
        def associate(type, name, opts = {}, &block)
          if block || opts[:dataset] || opts[:conditions] || (opts[:clone] && association_reflection(opts[:clone])[:prepared_statement] == false)
            opts = opts.merge(:prepared_statement=>false)
          end
          super(type, name, opts, &block)
        end
      end

      module InstanceMethods
        private

        # Return a bound variable hash that maps the keys in +ks+ (qualified by the +table+)
        # to the values of the results of sending the methods in +vs+.
        def association_bound_variable_hash(table, ks, vs)
          Hash[*ks.zip(vs).map{|k, v| [:"#{table}.#{k}", send(v)]}.flatten]
        end

        # Given an association reflection, return a bound variable hash for the given
        # association for this instance's values.
        def association_bound_variables(opts)
          case opts[:type]
          when :many_to_one
            association_bound_variable_hash(opts.associated_class.table_name, opts.primary_keys, opts[:keys])
          when :one_to_many
            association_bound_variable_hash(opts.associated_class.table_name, opts[:keys], opts[:primary_keys])
          when :many_to_many
            association_bound_variable_hash(opts.join_table_alias, opts[:left_keys], opts[:left_primary_keys])
          when :many_through_many
            association_bound_variable_hash(opts.final_reverse_edge[:alias], Array(opts[:left_key]), opts[:left_primary_keys])
          end
        end

        # Given an association reflection, return and cache a prepared statement for this association such
        # that, given appropriate bound variables, the prepared statement will work correctly for any
        # instance.
        def association_prepared_statement(opts)
          opts.send(:cached_fetch, :prepared_statement) do
            ps = _associated_dataset(opts, {}).unbind.first.prepare(opts.returns_array? ? :select : :first, :"smpsap_#{NEXT.call}")
            ps.log_sql = true
            ps
          end
        end

        # If a prepared statement can be used to load the associated objects, execute it to retrieve them.  Otherwise,
        # fall back to the default implementation.
        def _load_associated_objects(opts, dynamic_opts={})
          if !opts.can_have_associated_objects?(self) || dynamic_opts[:callback] || (ps = opts[:prepared_statement]) == false
            super
          else 
            if bv = association_bound_variables(opts)
              (ps || association_prepared_statement(opts)).call(bv)
            else
              super
            end
          end
        end
      end
    end
  end
end
