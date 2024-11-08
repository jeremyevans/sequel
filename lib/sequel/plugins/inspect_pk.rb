# frozen-string-literal: true

module Sequel
  module Plugins
    # The inspect_pk plugin includes the pk right next to the
    # model name in inspect, allowing for easily copying and
    # pasting to retrieve a copy of the object:
    #
    #   Album.with_pk(1).inspect
    #   #         default: #<Album @values={...}>
    #   # with inspect_pk: #<Album[1] @values={...}>
    #
    # Usage:
    #
    #   # Make all model instances include pk in inspect output
    #   Sequel::Model.plugin :inspect_pk
    #
    #   # Make Album instances include pk in inspect output
    #   Album.plugin :inspect_pk
    module InspectPk
      module InstanceMethods
        private

        # The primary key value to include in the inspect output, if any.
        # For composite primary keys, this only includes a value if all
        # fields are present.
        def inspect_pk
          if primary_key && (pk = self.pk) && (!(Array === pk) || pk.all?)
            pk
          end
        end

        # Include the instance's primary key in the output.
        def inspect_prefix
          if v = inspect_pk
            "#{super}[#{v.inspect}]"
          else
            super
          end
        end
      end
    end
  end
end
