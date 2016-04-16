# BSBM

This is a fork of the Berlin Sparql Benchmark (BSBM).

http://sourceforge.net/projects/bsbmtools/


## Running and Building

to generate the runtime artefacts and run the build, use:

'''
./gradle installDist
cd build/......
./generate
'''



## Additional features: 

Beware some of the paths are hardcoded, always generate files to the <bsbm-installation-folder>

### New serializers

Use: 
-s nqr
-s nqp
-s trig 

to generate quads with different graph associtations.


In order to generate roughly 1 million triples, using the slightly modified default quad generation schema, call:

'''
./generate -pc 2785 -s trig 
'''

### Auth generator

In order to create the authorization graph for a 1000 users, with 100 groups, call:
'''
generateAuth -fn ./auth.trig -uc 1000 -gc 100 
'''

The following files will be generated in the <bsbm-installation-folder> folder:
* auth.trig : The ontology, mapping groups to graphs
* auth.ldif : An LDAP dump of users and their groups

