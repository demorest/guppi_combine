#! /usr/bin/env python

import os
gpu = os.uname()[1]

# Cmd line
from optparse import OptionParser
parser = OptionParser()
parser.add_option("-c", "--create", 
        action="store_true", dest="create", default=False,
        help="Create database file")
parser.add_option("-f", "--dbfile", 
        action="store", dest="dbfile", 
        default="/data/gpu/partial/%s.idx" % gpu,
        help="Database file name")
parser.add_option("-t", "--time",
        action="store", dest="time", type="float",
        default=300.0,
        help="Ignore files newer than N seconds (default 300)")
(opt, arg) = parser.parse_args()

import sqlite3
conn = sqlite3.connect(opt.dbfile)

c = conn.cursor()

# Re-create whole file
if opt.create:
    c.execute("drop table files")

c.execute("create table if not exists " + \
        "files (name text unique, type text, mjd real, projid text, size int, merged int)")

datadir = "/data/gpu/partial/" + gpu

# Get file list
import glob
#import pyfits
import fitsio
for fname in glob.iglob(datadir + "/guppi*fits"):
    # Check file mod time
    if (time.time() - os.stat(fname).st_mtime) < opt.time:
        continue
    try:
        c.execute("select * from files where name=?", (fname,))
        if c.fetchone() == None:
            # Get info
            size = os.path.getsize(fname)

            # pyfits mysteriously chokes on some files...
            #arch = pyfits.open(fname)
            #type = arch[0].header['OBS_MODE']
            #mjd = arch[0].header['STT_IMJD'] \
            #        + arch[0].header['STT_SMJD']/86400.0
            #projid = arch[0].header['PROJID']
            #arch.close()

            # so use the fitsio wrapper instead
            arch = fitsio.FITS(fname)
            hdr = arch[0].read_header()
            type = hdr['OBS_MODE'].strip()
            mjd = hdr['STT_IMJD'] + hdr['STT_SMJD']/86400.0
            projid = hdr['PROJID'].strip()
            arch.close()

            print fname, type, mjd, projid, size
            # Insert new row
            c.execute("insert into files values (?,?,?,?,?,0)", 
                    (fname,type,mjd,projid,size))
    except:
        print "Error processing '%s'" % fname

# Prune non-existant entries
c.execute("select name from files")
for r in c.fetchall():
    fname = r[0]
    if os.path.exists(fname) == False:
        print fname
        c.execute("delete from files where name=?", (fname,))

conn.commit()
c.close()
