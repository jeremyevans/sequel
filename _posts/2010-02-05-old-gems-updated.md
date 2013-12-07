---
 layout: post
 title: Old Gems Updated
---

I've just pushed out new versions of the sequel_core and sequel_model gems.  Previously, installing those gems installed very old versions of sequel, and it's likely they didn't work at all.  The new gems are empty and just depend on sequel (with version >= 3.8.0).  Hopefully this will prevent some confusion if someone accidently installs sequel_core or sequel_model.
