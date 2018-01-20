# frozen_string_literal: true

module Sequel
  module Plugins
    # The pretty_print plugin provides models with a +pretty_print+ implementation that
    # can be used by tools like Pry. Assuming that we have a class Album with columns
    # id and name this provides the following functionality in Pry:
    #
    #  # Before:
    #  Artist.new
    #  # => #<Artist @values={}>
    #
    #  # After:
    #  Artist.new
    #  # => #<Artist:0x00007f8d1967f8b0 id: nil, name: nil>
    #  Artist.new(name: "Elvis")
    #  # => #<Artist:0x00007f8d1b413ef8 id: nil, name: "Elvis">
    #
    # Usage:
    #
    #   # Make all model subclasses use pretty_print (called before loading subclasses)
    #   Sequel::Model.plugin :pretty_print
    #
    #   # Make the Artist class use pretty_print
    #   Artist.plugin :pretty_print
    module PrettyPrint
      module InstanceMethods
        # Mimics the ActiveRecord's implementation
        def pretty_print(pp)
          return super if self.class.instance_method(:inspect).owner != Sequel::Model.instance_method(:inspect).owner

          pp.object_address_group(self) do
            column_names = self.class.columns.select { |name| @values.key?(name) || new? }.map(&:to_s)

            pp.seplist(column_names, proc { pp.text(",") }) do |column_name|
              column_value = self[column_name.to_sym]
              pp.breakable(" ")

              pp.group(1) do
                pp.text(column_name)
                pp.text(":")
                pp.breakable
                pp.pp(column_value)
              end
            end
          end
        end
      end
    end
  end
end
