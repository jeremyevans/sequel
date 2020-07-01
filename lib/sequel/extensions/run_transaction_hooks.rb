# frozen-string-literal: true
#
# The run_transaction_hooks extension allows for running after_commit or
# after_rollback extensions before commit or rollback.  It then removes
# the hook after running it, so it will not be run twice.
#
# This extension should only be used in transactional tests where the
# transaction always rolls back, to test the behavior of the after_commit
# and after_rollback hooks.  Any other usage is probably a bad idea.
#
# Example:
#
#   DB.extension :run_transaction_hooks
#   x = 1
#   DB.transaction(rollback: :always) do
#     DB.after_rollback{x = 3}
#     DB.after_commit{x = 2}
#
#     x # => 1
#     DB.run_after_rollback_hooks
#     x # => 3
#     DB.run_after_commit_hooks
#     x # => 2
#   end
#   x # => 2

#
class Sequel::Database
  module RunTransactionHooks
    # Run all savepoint and transaction after_commit hooks for the current transaction,
    # and remove the hooks after running them.
    # Options:
    # :server :: The server/shard to use.
    def run_after_commit_hooks(opts=OPTS)
      _run_transaction_hooks(:after_commit, opts)
    end

    # Run all savepoint and transaction after_rollback hooks for the current transaction,
    # and remove the hooks after running them.
    # Options:
    # :server :: The server/shard to use.
    def run_after_rollback_hooks(opts=OPTS)
      _run_transaction_hooks(:after_rollback, opts)
    end

    private

    def _run_transaction_hooks(type, opts)
      synchronize(opts[:server]) do |conn|
        unless h = _trans(conn)
          raise Sequel::Error, "Cannot call run_#{type}_hooks outside of a transaction"
        end

        if hooks = h[type]
          hooks.each(&:call)
          hooks.clear
        end

        if (savepoints = h[:savepoints])
          savepoints.each do |savepoint|
            if hooks = savepoint[type]
              hooks.each(&:call)
              hooks.clear
            end
          end
        end
      end
    end
  end

  register_extension(:run_transaction_hooks, RunTransactionHooks)
end
