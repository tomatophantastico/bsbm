# BSBM

This is a fork of the Berlin Sparql Benchmark (BSBM).

http://sourceforge.net/projects/bsbmtools/


## Additional features: 

Beware some of the paths are hardcoded, always generate files to the <bsbm-installation-folder>

### New serializers

Use: 
-s nqr
-s nqp
-s trig 

to generate quads with different graph associtations.

### Auth generator

use 

generateAuth 

to create the auth ontology and ldif files + a user list for basic http auth challenge.

Files will be generated in the <bsbm-installation-folder> folder.

