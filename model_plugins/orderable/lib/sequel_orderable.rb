module Sequel
  module Plugins
    module Orderable
      def self.apply(model, opts = {})
        # defaults
        opts[:field] ||= :position

        # custom behavior
        position_field = opts[:field]
        scope_field = opts[:scope]
        if scope_field
          model.dataset.order!(scope_field, position_field)
        else
          model.dataset.order!(position_field)
        end
      end

      module InstanceMethods
        def position
          @values[orderable_opts[:field]]
        end

        def at_position(p)
          position_field = orderable_opts[:field]
          if scope_field = orderable_opts[:scope]
            dataset.first(scope_field => @values[scope_field], position_field => p)
          else
            dataset.first(position_field => p)
          end
        end

        def prev(n = 1)
          target = position - n
          # XXX: error checking, negative target?
          return self if position == target
          at_position(target)
        end

        def next(n = 1)
          target = position + n
          at_position(target)
        end

        def move_to(pos)
          position_field = orderable_opts[:field]
          scope_field = orderable_opts[:scope]

          # XXX: error checking, negative pos?
          cur_pos = position
          return self if pos == cur_pos

          db.transaction do
            if pos < cur_pos
              ds = self.class.filter {position_field >= pos and position_field < cur_pos}
              ds.filter!(scope_field => @values[scope_field]) if scope_field
              ds.update(position_field => "#{position_field} + 1".lit)
            elsif pos > cur_pos
              ds = self.class.filter {position_field > cur_pos and position_field <= pos}
              ds.filter!(scope_field => @values[scope_field]) if scope_field
              ds.update(position_field => "#{position_field} - 1".lit)
            end
            set(position_field => pos)
          end
        end

        def move_up(n = 1)
          # XXX: position == 1 already?
          self.move_to(position-n)
        end

        def move_down(n = 1)
          # XXX: what if we're already at the bottom
          self.move_to(position+n)
        end

        def move_to_top
          self.move_to(1)
        end
        
        def move_to_bottom
          position_field = orderable_opts[:field]
          scope_field = orderable_opts[:scope]
          ds = dataset
          ds = ds.filter(scope_field => @values[scope_field]) if scope_field
          last = ds.select(:max[position_field] => :max).first.values[:max].to_i
          self.move_to(last)
        end
      end

    end
  end
end
