# filecatalog

It's a filesystem catalog written in python

It is slow and has only cli mode :-o

* it parses tar(.gz,.bz2) archives too...

The database abstraction layer is included from  Massimo Di Pierro's Web2py (http://www.web2py.com). Thank you!
<pre>
Usage: filecatalog.py [options]
 
Options:
  -h, --help            show this help message and exit
  -c, --catalog         catalog a disk. Default: False
  -a MERGE, --merge=MERGE
                        merge subdirectory content to the catalog. It have to
                        relative path to the root of the catalog! Default: ''
  -l, --list            list or search catalog. Default: True
  -m NAMESEARCH, --namesearch=NAMESEARCH
                        search the catalog for this filename. You can use '%'
                        as a wildcard.
  -d DIRECTORY, --dir=DIRECTORY
                        parse the directory as a root of a catalog
  -n STORAGENAME, --storagename=STORAGENAME
                        the name of the catalog. Default: MyHDD
  -s DATABASE, --database=DATABASE
                        the connection uri for the database. Look for it in
                        http://web2py.com/books/default/chapter/29/06/the-
                        database-abstraction-layer#Connection-strings--the-
                        uri-parameter- . Default: sqlite://filecatalog.sqlite
  -t DBMETADATA, --dbmetadata=DBMETADATA
                        the directory where DAL stores the structure of the
                        databas. Default: ./dbmetadata
  -v, --version         print the program version number

</pre>
