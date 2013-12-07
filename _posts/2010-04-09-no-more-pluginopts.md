---
 layout: post
 title: No More plugin_opts
---

I have <a href="http://github.com/jeremyevans/sequel/commit/95b730ad01e627f0f0b0872e235def0feb58f0fe">removed the creation of the Model plugin_opts class, instance, and dataset methods</a>.  This probably won't concern you unless you have a custom plugin you are keeping to yourself, as none of the built in plugins used these methods, and I couldn't fine an existing external user either.  It doesn't appear that these methods were documented outside of the specs, so I doubt this will have a negative effect on anyone.  However, if you are using one of the plugin_opts methods for an internal plugin, you'll have to define the necessary plugin_opts method(s) in your plugin's apply method after upgrading Sequel.
