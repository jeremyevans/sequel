# frozen-string-literal: true

module Sequel
  # TODO DOCS
  module DuplicateColumnsHandler
    def on_duplicate_columns(handler = nil, &block)
      if block_given?
        if handler.nil?
          handler = block
        else
          raise DuplicateColumnError, "Cannot provide both an argument and a block to on_duplicate_columns"
        end
      elsif handler.nil?
        raise DuplicateColumnError, "Must provide either an argument or a block to on_duplicate_columns"
      end

      ds = clone
      ds.opts[:on_duplicate_columns] = handler
      ds
    end

    def columns=(cols)
      if cols.uniq.size != cols.size
        handle_duplicate_columns(cols)
      end
      @columns = cols
    end

    private

    def handle_duplicate_columns(cols)
      message = "One or more duplicate columns present in #{cols.inspect}"

      case duplicate_columns_handler_type(cols)
      when :raise
        raise DuplicateColumnError, message
      when :warn
        warn message
      end
    end

    def duplicate_columns_handler_type(cols)
      handler = opts.fetch(:on_duplicate_columns){db.opts.fetch(:on_duplicate_columns, :warn)}

      if handler.respond_to?(:call)
        handler.call(cols)
      else
        handler
      end
    end
  end

  class DuplicateColumnError < Error
  end

  Dataset.register_extension(:duplicate_columns_handler, Sequel::DuplicateColumnsHandler)
end
