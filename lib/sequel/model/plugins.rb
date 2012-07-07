module Sequel
  # Empty namespace that plugins should use to store themselves,
  # so they can be loaded via Model.plugin.
  #
  # Plugins should be modules with one of the following conditions:
  # * A singleton method named apply, which takes a model,
  #   additional arguments, and an optional block.  This is called
  #   the first time the plugin is loaded for this model (unless it was
  #   already loaded by an ancestor class), before including/extending
  #   any modules, with the arguments
  #   and block provided to the call to Model.plugin.
  # * A module inside the plugin module named InstanceMethods,
  #   which will be included in the model class.
  # * A module inside the plugin module named ClassMethods,
  #   which will extend the model class.
  # * A module inside the plugin module named DatasetMethods,
  #   which will extend the model's dataset.
  # * A singleton method named configure, which takes a model,
  #   additional arguments, and an optional block.  This is called
  #   every time the Model.plugin method is called, after including/extending
  #   any modules.
  module Plugins
  end
end
