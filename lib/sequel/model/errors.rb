module Sequel
  class Model
    # Errors represents validation errors, a simple hash subclass
    # with a few convenience methods.
    class Errors < ::Hash
      ATTRIBUTE_JOINER = ' and '.freeze

      # Assign an array of messages for each attribute on access
      def [](k)
        has_key?(k) ? super : (self[k] = [])
      end

      # Adds an error for the given attribute.
      def add(att, msg)
        self[att] << msg
      end

      # Return the total number of error messages.
      def count
        values.inject(0){|m, v| m + v.length}
      end
      
      # Return true if there are no error messages, false otherwise.
      def empty?
        count == 0
      end
      
      # Returns an array of fully-formatted error messages.
      def full_messages
        inject([]) do |m, kv| 
          att, errors = *kv
          errors.each {|e| m << "#{Array(att).join(ATTRIBUTE_JOINER)} #{e}"}
          m
        end
      end
      
      # Returns the array of errors for the given attribute, or nil
      # if there are no errors for the attribute.
      def on(att)
        self[att] if has_key?(att)
      end
    end
  end
end
