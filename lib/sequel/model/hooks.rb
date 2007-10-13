module Sequel
  class Model

    class ChainBroken < RuntimeError # :nodoc:
    end

    # This Hash translates verbs to methodnames used in chain manipulation
    # methods.
    VERB_TO_METHOD = {:prepend => :unshift, :append => :push}

    # Returns @hooks which is an instance of Hash with its hook identifier
    # (Symbol) as key and the chain of hooks (Array) as value.
    #
    # If it is not already set it'll be with an empty set of hooks.
    # This behaviour will change in the future to allow inheritance.
    #
    # For the time being, you should be able to do:
    #
    #   class A < Sequel::Model(:a)
    #     before_save { 'Do something...' }
    #   end
    #
    #   class B < A
    #     @hooks = superclass.hooks.clone
    #     before_save # => [#<Proc:0x0000c6e8@(example.rb):123>]
    #   end
    #
    # In this case you should remember that the clone doesn't create any new
    # instances of your chains, so if you change the chain here it changes in
    # its superclass, too.
    def self.hooks
      @hooks ||= Hash.new { |h, k| h[k] = [] }
    end

    # Adds block to chain of Hooks for <tt>:before_save</tt>.
    # It can either be prepended (default) or appended.
    #
    # Returns the chain itself.
    #
    # Valid verbs are <tt>:prepend</tt> and <tt>:append</tt>.
    def self.before_save(verb = :prepend, &block)
      hooks[:before_save].send VERB_TO_METHOD.fetch(verb), block if block
      hooks[:before_save]
    end
    # Adds block to chain of Hooks for <tt>:before_create</tt>.
    # It can either be prepended (default) or appended.
    #
    # Returns the chain itself.
    #
    # Valid verbs are <tt>:prepend</tt> and <tt>:append</tt>.
    def self.before_create(verb = :prepend, &block)
      hooks[:before_create].send VERB_TO_METHOD.fetch(verb), block if block
      hooks[:before_create]
    end
    # Adds block to chain of Hooks for <tt>:before_update</tt>.
    # It can either be prepended (default) or appended.
    #
    # Returns the chain itself.
    #
    # Valid verbs are <tt>:prepend</tt> and <tt>:append</tt>.
    def self.before_update(verb = :prepend, &block)
      hooks[:before_update].send VERB_TO_METHOD.fetch(verb), block if block
      hooks[:before_update]
    end
    # Adds block to chain of Hooks for <tt>:before_destroy</tt>.
    # It can either be prepended (default) or appended.
    #
    # Returns the chain itself.
    #
    # Valid verbs are <tt>:prepend</tt> and <tt>:append</tt>.
    def self.before_destroy(verb = :prepend, &block)
      hooks[:before_destroy].send VERB_TO_METHOD.fetch(verb), block if block
      hooks[:before_destroy]
    end

    # Adds block to chain of Hooks for <tt>:after_save</tt>.
    # It can either be prepended or appended (default).
    #
    # Returns the chain itself.
    #
    # Valid verbs are <tt>:prepend</tt> and <tt>:append</tt>.
    def self.after_save(verb = :append, &block)
      hooks[:after_save].send VERB_TO_METHOD.fetch(verb), block if block
      hooks[:after_save]
    end
    # Adds block to chain of Hooks for <tt>:after_create</tt>.
    # It can either be prepended or appended (default).
    #
    # Returns the chain itself.
    #
    # Valid verbs are <tt>:prepend</tt> and <tt>:append</tt>.
    def self.after_create(verb = :append, &block)
      hooks[:after_create].send VERB_TO_METHOD.fetch(verb), block if block
      hooks[:after_create]
    end
    # Adds block to chain of Hooks for <tt>:after_update</tt>.
    # It can either be prepended or appended (default).
    #
    # Returns the chain itself.
    #
    # Valid verbs are <tt>:prepend</tt> and <tt>:append</tt>.
    def self.after_update(verb = :append, &block)
      hooks[:after_update].send VERB_TO_METHOD.fetch(verb), block if block
      hooks[:after_update]
    end
    # Adds block to chain of Hooks for <tt>:after_destroy</tt>.
    # It can either be prepended or appended (default).
    #
    # Returns the chain itself.
    #
    # Valid verbs are <tt>:prepend</tt> and <tt>:append</tt>.
    def self.after_destroy(verb = :append, &block)
      hooks[:after_destroy].send VERB_TO_METHOD.fetch(verb), block if block
      hooks[:after_destroy]
    end

    # Evaluates specified chain of Hooks through <tt>instance_eval</tt>.
    def run_hooks(key)
      model.hooks[key].each { |h| instance_eval &h }
    end

  end
end
