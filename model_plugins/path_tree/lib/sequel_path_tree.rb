module Sequel
  module Plugins

    # A Path Tree that stores the path for each element, making it able to retrieve
    # all children (including indirect) with one query while being more flexible
    # and easier than a nested set.
    #
    # == Usage ==
    # +is :path_tree, {}+
    # Please don't forget the empty hash, it's needed with the current sequel.
    # When you apply the plugin with an empty option hash, a default path column
    # named 'path' is created if not present, using a default delimiter of '.'
    #
    # TODO Document customization options
    #
    # == Internal structure ==
    # This plugin needs a path column, by default its name is "path". In the path
    # column, the primary keys of the ancestors are stored. The keys are separated
    # by delimiters, being "." by default. Note that there has to be a trailing
    # delimiter on each non-empty path. The idea is to make it easy finding children
    # by just taking the current node's path, appending our own primary key and looking
    # for records that have path "LIKE" that.
    #
    module PathTree

      # Apply the plugin to the model.
      def self.apply(model, options)
        options.merge!({
          :path_column => :path,
          :path_delimiter => '.'
        }.merge(options))
        
        unless model.columns.include? options[:path_column]
          model.db.add_column model.table_name, options[:path_column], :string
        end
        
      end

      module InstanceMethods
        
        # Returns a dataset of the direct children of this node.
        # Only available for saved records. The resulting dataset allows
        # appending of new children through "<<"
        def children
          unless new_record?
            ds = self.class.filter do
              tree_path_column == child_path
            end.extend(ChildrenMethods)
            ds.parent = self
            ds
          end
        end

        # Returns a dataset of direct and indirect children of this node.
        # Only available for saved records. The resulting dataset allows
        # appending of new children through "<<".
        def descendants
          unless new_record?
            ds = self.class.filter do
              tree_path_column.like "'#{child_path + "%"}'".lit
            end.extend(ChildrenMethods)
            ds.parent = self
            ds
          end
        end

        # Returns a dataset of this node, direct and indirect children.
        # Only available for saved records.
        # TODO Can this be refactored to reuse descendants()?
        def full_set
          unless new_record?
            self.class.filter do
              (tree_path_column.like "'#{child_path + "%"}'".lit) || (self.class.primary_key == self.pk)
            end
          end
        end

        # Shortcut method to add children
        # TODO Alias any old methods and call them if it's a different kind
        def <<(child)
          if (!new_record? && child.is_a? self.class)
            self.children << child
          end
        end

        # Makes the node a root.
        # If the node is already saved, the changes are applied immediately and
        # all the node's descendants are updated accordingly. Make sure to reload
        # any affected model object (the node itself is automatically reloaded).
        def make_root!
          if new_record?
            self[path_tree_opts[:path_column]] = ""
          else
            prefix = tree_path
            full_set.update(
              tree_path_column => "substr(#{tree_path_column}, #{prefix.size+1}, -1)".lit
            )
            self.reload
          end
        end

        # Returns the parent, if any
        def parent
          self.class[parent_id]
        end

        # Returns the root. If the node is a root, +self+ is returned
        def root
          if root_id == self.pk
            self
          else
            self.class[root_id]
          end
        end

        # Returns a dataset of the ancestors (not including the node itself)
        def ancestors
          self.class.filter(self.class.primary_key => ancestor_ids)
        end

        # Returns the level of this node in the tree, root nodes being at level 0
        def level
          ancestor_ids.size
        end

        # Returns an array of the ids of the nodes ancestors.
        # This can be retrieved without a query.
        def ancestor_ids
          tree_path.split path_tree_opts[:path_delimiter]
        end

        # Returns the id of the root of this node
        # This can be retrieved without a query.
        def root_id
          ancestor_ids.first || self.pk
        end

        # Returns the id of the parent of this node
        # This can be retrieved without a query.
        def parent_id
          ancestor_ids.last
        end

        # The path that children of this node should have
        def child_path
           tree_path + self.pk.to_s + path_tree_opts[:path_delimiter]
        end

        protected

        # The column name which holds the path
        def tree_path_column
          "#{self.class.table_name}__#{path_tree_opts[:path_column]}".to_sym
        end

        # The path of this node
        def tree_path
          self[path_tree_opts[:path_column]] || ""
        end

        # Convenience methods to get added to children datasets
        module ChildrenMethods

          # The module adds a reference to the parent node of the contained children. Clients should
          # not change this 
          # TODO Hide at least the setter from clients.
          attr_accessor :parent

          # Append a new to child. If the child is a new record, it also get's saved by doing this.
          def <<(child)
            if (child.is_a? parent.class)
              if (child.new_record?)
                # If it's a new record it doesn't have children, so we just set the path correctly
                child[parent.path_tree_opts[:path_column]] = parent.child_path
                # We save the child because it's probably convenient to clients and, if we would not
                # save it here, the set semantics of the +children+ would break, cause you could add
                # things to +children+, but they would not appear in the set until the element was
                # saved, so this makes more sense.
                child.save
              else
                # If it's a saved record, we also move the descendants
                old_prefix = child[parent.path_tree_opts[:path_column]]
                # TODO This might not work with MySQL or other DBs since it uses non-standard functions,
                # so it would be helpful to have the necessary substr() syntax in Sequel adapted to either
                # substr for SQLite and SUBSTRING for MySQL. A short-time workaround might be to implement
                # manual iteration through the descendants as a fallback that can be enabled through the
                # option hash.
                child.full_set.update(
                  parent.path_tree_opts[:path_column] => 
                  "'#{parent.child_path}' || substr(#{parent.path_tree_opts[:path_column]}, #{old_prefix.size+1}, -1)".lit
                )
                # The child (and any descendand) need to be reloaded. We can do that only for the child,
                # so it does not make a lot of sense. On the other hand, you get strange results when you
                # continue working with the child object, or even add it to another children set without
                # reloading it first. TODO Maybe a better solution would be to join in the child or load
                # the child before appending it.
                child.reload
              end
            end
          end


        end
      end

      module ClassMethods

        # Returns a dataset of the root nodes
        def roots
          filter { path_tree_opts[:path_column] == nil || path_tree_opts[:path_column] == "" }
        end

      end


    end
  end
end