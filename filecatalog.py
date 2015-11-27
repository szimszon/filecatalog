#!/usr/bin/env python

import os
import sys
from gluon import DAL, Field
from gluon.validators import *
import datetime
import magic

class config():
    def __init__(self):
        """
            Parse commandline parameters
        """
        from optparse import OptionParser
        parser = OptionParser()
        parser.add_option("-c", "--catalog",
                  action="store_true", dest="catalog", default=False,
                  help="catalog a disk. Default: False")
        parser.add_option("-a", "--merge",
                  action="store", dest="merge", default=None,
                  help="merge subdirectory content to the catalog. It have to relative path to"+\
                        " the root of the catalog! Default: None")
        parser.add_option("-l", "--list",
                  action="store_true", dest="list", default=False,
                  help="list or search catalog. Default: True")
        parser.add_option("-m", "--namesearch",
                  action="store", dest="namesearch", default="%",
                  help="search the catalog for this filename. You can use '%' as a wildcard.")
        parser.add_option("-f", "--fieldsearch",
                  action="store", dest="fieldsearch", default=None,
                  help="search the catalog for sets of file properties. You can list as many field"+\
                  " as you wish. Separate it with ';'. Example: -f 'description=text file;uid=0'"+\
                  " Default: None")        
        parser.add_option("-d", "--dir",
                  action="store", dest="directory", default="./",
                  help="parse the directory as a root of a catalog")
        parser.add_option("-n", "--storagename",
                  action="store", dest="storagename", default="MyHDD",
                  help="the name of the catalog. Default: MyHDD")
        parser.add_option("-s", "--database",
                  action="store", dest="database", default="sqlite://filecatalog.sqlite",
                  help="the connection uri for the database. Look for it in "+\
                    "http://web2py.com/books/default/chapter/29/06/the-database-abstraction-layer"+\
                    "#Connection-strings--the-uri-parameter- . Default: sqlite://filecatalog.sqlite")
        parser.add_option("-t", "--dbmetadata",
                  action="store", dest="dbmetadata", default="./dbmetadata",
                  help="the directory where DAL stores the structure of the database. Default: ./dbmetadata")
        parser.add_option("-v", "--version",
                  action="store_true", dest="version", default=False,
                  help="print the program version number")

        (self.options, self.args) = parser.parse_args()
    
    def get_option(self,o=None):
        """
            Return the parsed parameter's value
        """
        if not o:
            return self.options
        else:
            return self.options.__dict__[o]
    
    def get_arg(self):
        """
            Return the parsed arguments
        """
        return self.args
    
    def get_db(self):
        """
            Return the connected db
        """
        if not os.path.exists(self.get_option('dbmetadata')):
            os.makedirs(self.get_option('dbmetadata'))
        db=DAL(self.get_option('database'),lazy_tables=True,folder=self.get_option("dbmetadata"))
        db.define_table('storages',
                    Field('storagename','string'),
                    Field('creation_ts','datetime',
                          default=datetime.datetime.now()),
                    Field('modified_ts','datetime',
                          default=datetime.datetime.now(),
                          update=datetime.datetime.now()),
                    )
        db.define_table('files',
                    Field('storages_id',db.storages),
                    Field('parent_id','reference files'),
                    Field('filename','string'),
                    Field('description','string'),
                    Field('mime','string'),
                    Field('ftype','string'),
                    Field('mode','integer'),
                    Field('inode','integer'),
                    Field('dev','string'),
                    Field('nlink','integer'),
                    Field('uid','integer'),
                    Field('gid','integer'),
                    Field('size','integer'),
                    Field('ctime','datetime'),
                    Field('mtime','datetime'),
                    Field('atime','datetime'),
                    )
        return db


class catalog():
    def __init__(self,cfg):
        """
            This class makes the cataloging of the directory recursively
        """
        self.cfg=cfg
        self.dp=display_progress()
        self.top=self.cfg.get_option('directory')
        self.compressions=self.get_compressions_processing_catalog()
        self.db=self.cfg.get_db()
        self.db.storages.update_or_insert(storagename=self.cfg.get_option('storagename'))
        self.storages_id=self.db(self.db.storages.storagename==self.cfg.get_option('storagename')).select(
            self.db.storages.id
                                                                                                    ).first().id
    
    def get_compressions_processing_catalog(self):
        """
            This is a list of file extension list and a process name to catalog the compressed file
        """
        return [
                [[".tgz",".tar",".tar.gz","tar.bz2"],self.walktar]
                ]
    
    def do_catalog(self):
        """
            Proceed with the catalog
        """
        db=self.db
        if not self.cfg.get_option('merge'):
            print "Create or rescreate catalog: "+self.cfg.get_option('storagename')
            db(db.files.storages_id==self.storages_id).delete()
            parent_id=None
            top=None
        else:
            print "Merge or recreate ["+self.cfg.get_option("merge")+"] dir into "+self.cfg.get_option('storagename')+\
            " catalog"
            parent_id=self.get_row_id_for_path(self.cfg.get_option('merge'))
            db(db.files.parent_id==parent_id).delete()
            top=os.path.join(self.top,self.cfg.get_option('merge'))
        self.walktree(top, parent_id)
        self.commit()

    def get_row_id_for_path(self,path,parent_id=None):
        """
            Return the db row.id for the specified path. It walk along the path searching for childs and
            return the last child's id
        """
        db=self.db
        for p in path.split(os.path.sep):
            if p=='' or p=='.':
                continue
            if p=='..':
                raise Exception('You must not use ".." in path!')
            rows=db((db.files.storages_id==self.storages_id)&
                   (db.files.filename==p)&
                   (db.files.parent_id==parent_id)).select(db.files.id)
            if len(rows)>1:
                raise Exception('Something is inconsistent in db. There is a multiple ['+p+\
                                '] filename with the same ['+parent_id+'] parent_id!')
            if len(rows)<1:
                raise Exception(' ['+p+'] filename can\'t be found with the ['+parent_id+'] parent_id!')
            parent_id=rows.first().id
        return parent_id
    
    def walktree(self,top=None,parent_id=None):
        """
            Walk through the directory tree and catalog it
        """
        if not top:
            top=self.top
        self.dp.display_dir(top)
        for f in os.listdir(top):
            pathname = os.path.join(top, f)
            if os.path.islink(pathname):
                continue
            if os.path.isdir(pathname):
                # It's a directory, recurse into it
                new_parent_id=self.storefile(self.fileprop(pathname),parent_id)
                try:
                    self.walktree(pathname, new_parent_id)
                except:
                    pass
            elif os.path.isfile(pathname):
                fileproperties=self.fileprop(pathname)
                file_parent_id=self.storefile(fileproperties,parent_id)
                for compression, store in self.compressions:
                    if str(os.path.splitext(pathname)[1]) in compression:
                        store(pathname,file_parent_id)

    def walktar(self,tarfilename,parent_id):
        """
            Walk through the tar archive and catalog it
        """
        self.dp.display_dir(tarfilename)
        cache_parent_id={}
        import tarfile
        tarfile_obj=tarfile.open(tarfilename)
        tar_member=tarfile_obj.next()
        while tar_member:
            self.storefile(self.tarprop(tar_member),self.get_tar_parent_id(tar_member.name,parent_id,cache_parent_id))
            tar_member=tarfile_obj.next()

    def get_tar_parent_id(self,filepath,parent_id,cache_parent_id):
        """
            Return the parent id for the specified file name.
            I think the tar file contains the files in random order so we always need to search for a
            directory structure in db.
        """
        dirname=os.path.dirname(filepath)
        filename=os.path.basename(dirname)
        if len(dirname)>0:
            parent_id=self.get_tar_parent_id(dirname, parent_id, cache_parent_id)
        q=(self.db.files.parent_id==parent_id)&\
                       (self.db.files.filename==filename)
        qstr=str(q)
        if qstr in cache_parent_id:
            return cache_parent_id[qstr]
        record=self.db((self.db.files.parent_id==parent_id)&\
                       (self.db.files.filename==filename)).select(self.db.files.id).first()
        if record:
            cache_parent_id[qstr]=int(record.id)
            return int(record.id)
        else:
            record_id=self.storefile(self.tardirprop(filename), parent_id)
            cache_parent_id[qstr]=int(record_id)
            return int(record_id)
    
    def tardirprop(self,filename):
        """
            object for holding data about the tar directory for storefiles process
        """
        prop=dict(
            mode=0,
            inode=0,
            dev=0,
            nlink=0,
            uid=0,
            gid=0,
            size=0,
            atime=None,
            mtime=None,
            ctime=None
                  )
        prop['filename']=filename
        prop['description']=''
        prop['mime']=''
        prop['ftype']='dir'
        return prop

    
    def tarprop(self,tarfile_obj):
        """
            object for holding data about the tar file for storefiles process
        """
        prop=dict(
            mode=tarfile_obj.mode,
            inode=0,
            dev=0,
            nlink=0,
            uid=tarfile_obj.uid,
            gid=tarfile_obj.gid,
            size=tarfile_obj.size,
            atime=None,
            mtime=datetime.datetime.fromtimestamp(tarfile_obj.mtime),
            ctime=None
                  )
        prop['filename']=os.path.basename(tarfile_obj.name)
        prop['description']=''
        prop['mime']=''
        if tarfile_obj.islnk():
            prop['ftype']='lnk'
        elif tarfile_obj.isdir():
            prop['ftype']='dir'
        elif tarfile_obj.isfile():
            prop['ftype']='file'
        else:
            prop['ftype']='unknown'
        return prop
    
    def fileprop(self,pathname):
        """
            object for holding data about the file for storefiles process
        """
        (mode,ino,dev,nlink,uid,gid,size,atime,mtime,ctime)=os.stat(pathname)
        prop=dict(
            mode=mode,
            inode=ino,
            dev=dev,
            nlink=nlink,
            uid=uid,
            gid=gid,
            size=size,
            atime=datetime.datetime.fromtimestamp(atime),
            mtime=datetime.datetime.fromtimestamp(mtime),
            ctime=datetime.datetime.fromtimestamp(ctime)
                  )
        prop['filename']=os.path.basename(pathname)
        prop['description']=magic.from_file(pathname)
        prop['mime']=magic.from_file(pathname,mime=True)
        prop['ftype']=None
        for compression, unused in self.compressions:
            if str(os.path.splitext(pathname)[1]) in compression:
                prop['ftype']='cmp'
                break
        if not prop['ftype']:
            if os.path.islink(pathname):
                prop['ftype']='lnk'
            elif os.path.isdir(pathname):
                prop['ftype']='dir'
            elif os.path.isfile(pathname):
                prop['ftype']='file'
            else:
                prop['ftype']='unknown'
        return prop
    
    def storefile(self,prop,parent_id=None):
        """
            storing file/directory properties into db
        """
        if not 'parent_id' in prop and parent_id:
            prop['parent_id']=parent_id
        self.dp.display_number()
        record_id=self.db.files.update_or_insert(
            storages_id=self.storages_id,
            **prop
                                       )
        return record_id
        
    def commit(self):
        """
            commit the changes into the db
        """
        self.db.commit()

class display_progress():
    
    def __init__(self):
        """
            This class do some processing feedback to the screen
        """
        self.dir=None
        self.number=0
    
    def set_dir(self,name):
        """
            set the directory name that is actually processed
        """
        if len(name)>100:
            name=name[:50]+' ... '+name[-45:]
        self.dir=name
        self.number=0
    
    def plus_number(self):
        """
            elevate the number of processed files in the directory
        """
        self.number+=1
    
    def display_dir(self,name):
        """
            display the currently processed directory in screen
        """
        self.set_dir(name)
        txt='['+self.dir+']'
        sys.stdout.write(txt+' '*(100-len(txt))+'\n')
        sys.stdout.flush()
    
    def display_number(self):
        """
            elevate the number of processed files in directory and display it in screen
        """
        self.plus_number()
        number=str(self.number)
        sys.stdout.write(' ----- ['+number+']'+' '*(30-len(number))+'\r')
        sys.stdout.flush()
        
class listfiles():
    
    def __init__(self,cfg):
        """
            This class manages the display-ing of the content of the catalog
        """
        self.cfg=cfg
        self.db=self.cfg.get_db()
        self.namesearch=self.cfg.get_option('namesearch')
        self.fieldsearch=self.cfg.get_option('fieldsearch')
        self.storages_id=self.db(self.db.storages.storagename==str(self.cfg.get_option("storagename"))).\
            select(self.db.storages.id).first().id
        q=None
        self.storage=self.get_search_query(q)
    
    def tree(self,q=None,parent_id=None,level=0):
        """
            display the resulted directory and files tree corresponding with to the 'q' query
        """
        db=self.db
        if not q:
            q=self.storage
        rows=db(q&(db.files.parent_id==parent_id)).select(db.files.ALL,orderby=[db.files.parent_id,db.files.filename])
        for row in rows:
            print self.print_row(row,level)
            if db(db.files.parent_id==row.id).select():
                self.tree(q,row.id, level=level+1)
                
    def print_row(self,row,level):
        """
            print one row of the result
        """
        prefix=' | '*level
        fname=row.filename
        if len(fname)>20:
            fname=fname[:10]+'...'+fname[-7:]
        fname=fname[:20]+' '*(20-len(row.filename[:20]))
        if row.size>1024*1024:
            size=row.size/1024/1024
            spostfix=' MB'
        elif row.size>1024:
            size=row.size/1024
            spostfix=' KB'
        else:
            size=row.size
            spostfix=' B '
        size=' '*(10-len(str(size)))+''+str(size)+spostfix
        mode=str(row.mode)+' '*(5-len(str(row.mode)))
        uid=str(row.uid)+' '*(7-len(str(row.uid)))
        gid=str(row.gid)+' '*(7-len(str(row.gid)))
        ctime=str(row.ctime)
        mtime=str(row.mtime)
        description=row.description
        
        return prefix+' +- '+fname+''+size+' '+uid+' '+gid+' '+mode+' [ '+ctime+' ] [ '+mtime+' ] -- '+description

    def get_related_rows(self,r,ids=[]):
        """
            return ids of related item for the items with a specified pattern you searched 
        """
        pif=self.db((self.db.files.storages_id==self.storages_id)&\
                    (self.db.files.id==r.parent_id)).select(self.db.files.parent_id).first()
        if pif.parent_id:
            ids=self.get_related_rows(pif, ids)
            if not pif.parent_id in ids:
                ids.append(str(pif.parent_id))
        return ids      

    def get_search_query(self,q=None):
        """
            Return the constructed query for namesearch and/or fieldsearch
        """
        db=self.db
        namesearch=self.namesearch
        fieldsearch=self.fieldsearch
        if not q:
            q=(db.files.storages_id==self.storages_id)
        if namesearch!="%":
            if not q:
                q=(db.files.filename.like(namesearch))
            else:
                q=q&(db.files.filename.like(namesearch))
        if fieldsearch:
            for fsearch in fieldsearch.split(';'):
                fname,value=fsearch.split('=')
                if not q:
                    q=(db.files[fname]==value)
                else:
                    q=q&(db.files[fname]==value)
        or_q=None
        if namesearch!='%' or fieldsearch:
            related_ids=[]
            for r in db(q).select(self.db.files.ALL):
                if r.parent_id:
                    related_ids.extend(self.get_related_rows(r))
                    related_ids.append(r.parent_id)
            if not or_q:
                or_q=(db.files.id.belongs(related_ids))
            else:
                or_q=or_q|(db.files.id.belongs(related_ids))
        if or_q:
            q=q|or_q
        return q
        
def main():
    
    cfg=config()
    if cfg.get_option("version"):
        print "Version: 2015112704"
        sys.exit(0)
    if cfg.get_option('catalog'):
        ctl=catalog(cfg)
        ctl.do_catalog()
    else:
        lst=listfiles(cfg)
        lst.tree()

if __name__ == "__main__":
    main()
    