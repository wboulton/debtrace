# debtrace
this is the code for the debtrace tool which for now fails to find paths\

## updates/notes
New method for finding paths: 
 - find all publications that match a given package name and version
 - find all sources with the same name as the publication results and LIKE the version
 - find a buildinfo that has the same source_id
When a path is found it immediately terminates.

Tested and works for `works for 0ad 0.0.23.1`
