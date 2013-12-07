---
 layout: post
 title: New :eager_loader API
---

Sequel has supported the :eager_loader association option <a href="http://github.com/jeremyevans/sequel/commit/a226fadd5cd3fcdae1a5972cfcc43da840371d78">since June 2008</a>, as a way of specifying custom behavior when eagerly loading associations.  As far as I know, Sequel is still the only ruby ORM that supports custom eager loading.  I recently <a href="http://github.com/jeremyevans/sequel/commit/14e9bcd1840e90464cb399d7c1b9319c782b63d4">made an addition to the :eager_loader API</a>, that allows some additional flexibility.

Previously, the :eager_loader option took a proc that accepts three arguments.  Sequel still allows this for backwards compatibility, but the new recommended way is to use a single hash argument.  If the :eager_loader proc only accepts a single argument, Sequel will pass a hash with the :key_hash, :rows, and :associations options that correspond to the previous 3 arguments.  Also in the hash will be a key of :self, which refers to the dataset doing the eager loading.

The new :self option is necessary in order to give eager loaders the ability to tell which dataset is doing the eager load, and customize the eager load based on that dataset.  Sequel started using this new API format internally in order to <a href="http://github.com/jeremyevans/sequel/commit/9d42ec2830329bb53c43adf578d55573bb61743d">enable the sharding plugin to work with eager loading</a>.

Since I knew that the :self option would have required an API change, I had two choices.  I could either add it as a fourth argument, or I could choose to use a completely different format.  There were two reasons I took the latter approach and decided to use a single hash.  First, if I had added it as a fourth argument, what would I do when I had something else I wanted to add?  I would have to add a fifth argument and then support three different APIs.  By using a single hash, I can keep the API the same, and still add more keys later if they are necessary.  Secondly, if you've ever seen <a href="http://onestepback.org/">Jim Weirich</a> <a href="http://mwrc2009.confreaks.com/14-mar-2009-18-10-the-building-blocks-of-modularity-jim-weirich.html">talk about connascence</a>, you should be aware that its often better to use keyword arguments instead of relying on argument order for a large number of arguments.

In terms of principles, the first reason for this API change is flexibility, and the second reason is simplicity.  Combined with the power that custom eager loading gives you, you have the Triforce of Sequel: simplicity, flexibility, power.
