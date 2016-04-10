# frozen-string-literal: true

module Sequel
  # TODO DOCS
  module DuplicateColumnsHandler

    # TODO DOCS
    module DatasetMethods
      def on_duplicate_columns(handler = nil, &block)
        if block_given?
          if handler.nil?
            handler = block
          else
            raise Error, "Cannot provide both an argument and a block to on_duplicate_columns"
          end
        elsif handler.nil?
          raise Error, "Must provide either an argument or a block to on_duplicate_columns"
        end

        ds = clone
        ds.instance_variable_set(:@on_duplicate_columns, handler)
        ds
      end

      def columns=(cols)
        @columns = cols
        if cols.uniq.size != cols.size
          handle_duplicate_columns
        end
        cols
      end

      private

      def handle_duplicate_columns
        message = "One or more duplicate columns present in #{@columns.inspect}"

        case duplicate_columns_handler_type
        when :raise
          raise Error.new(message)
        when :warn
          warn message
        end
      end

      def duplicate_columns_handler_type
        handler = if @on_duplicate_columns
          @on_duplicate_columns
        else
          db.opts[:on_duplicate_columns] || :warn
        end

        if handler.respond_to?(:call)
          handler.call(@columns)
        else
          handler
        end
      end
    end

    class Error < Sequel::Error
    end
  end

  Dataset.register_extension(:duplicate_columns_handler, Sequel::DuplicateColumnsHandler::DatasetMethods)
end
