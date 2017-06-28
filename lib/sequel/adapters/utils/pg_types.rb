# frozen-string-literal: true

Sequel::Deprecation.deprecate("requiring sequel/adapters/utils/pg_types", "This file should no longer be required, use Database#conversion_procs to modify conversion procs for a Database instance")
Sequel.require 'adapters/shared/postgres'
