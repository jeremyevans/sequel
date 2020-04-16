# frozen-string-literal: true
#
# The fiber_concurrency extension changes the default concurrency
# primitive in Sequel to be Fiber.current instead of Thread.current.
# This is the value used in various hash keys to implement safe
# concurrency (thread-safe concurrency by default, fiber-safe
# concurrency with this extension.  It can be enabled via:
#
#   Sequel.extension :fiber_concurrency
#   
# Related module: Sequel::FiberConcurrency

require 'fiber'

module Sequel
  module FiberConcurrency
    # Make the current concurrency primitive be Fiber.current.
    def current
      Fiber.current
    end
  end

  extend FiberConcurrency
end
