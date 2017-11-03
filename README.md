# Dit -- Efficient image storaging inspired by Git

Dit is optimized to store directory contains container images.
File and directory metadata are stored within a PostgreSQL database for simple management.
File content is deduplicated and stored within a local LevelDB.

Dit supports currently two major commands: 

* **commit**: recording the files and their content from a given directory
* **checkout**: rebuild a recorded system into the wanted directory

Furthermore, Dit can store additional artifacts for systems that are deduplicated, too.


### Why?

Containers build by Continuous Integration systems a very similar. Only some files change between different builds.
To store and built systems efficiently at least the file content should be deduplicated.
Git provides a good storage design for this, but lacks support for file metadata like user, group ... This prevent using Git for storaging system images.
Filesystems like OverlayFS, ZFS or Btrfs support some kind of deduplication but are designed for a different usage case and deduplication not all content or in an inefficient manner.


### Status

Dit is used internally for a continuous integration system.
Besides some known limitations it works well.



### Limitations

* Imroved database schema: the current schema does not duplicate file metadata. Storing many systems requires some PostgreSQL space and reduces performance
* LevelDB sync: to support usage between multiple hosts LevelDB entries must be transfered between multiple hosts


### Dependencies

* haskell
* cabal recommanded
* postgresql
* leveldb
