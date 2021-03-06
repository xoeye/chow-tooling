#!/usr/bin/env python
"""
Adapted from hashdeep, by anatoly techtonik.

https://gist.github.com/techtonik/5175896
"""

import argparse
import hashlib
import os
import os.path as osp
import re

"""
Build recursive hash of files in directory tree in hashdeep format.


Hashdeep format description:
http://md5deep.sourceforge.net/start-hashdeep.html

hashdeep.py differences from original hashdeep:

 - if called without arguments, automatically starts to build
   recursive hash starting from the current directory
   (original hashdeep waits for the output from stdin)
 - uses only sha256 (original uses md5 and sha256)
 - uses relative paths only (original works with absolute)


hashdeep.py output example:

$ hashdeep.py
%%%% HASHDEEP-1.0
%%%% size,sha256,filename
##
## $ hashdeep.py
##
5584,28a9b958c3be22ef6bd569bb2f4ea451e6bdcd3b0565c676fbd3645850b4e670,dir/config.h
9236,e77137d635c4e9598d64bc2f3f564f36d895d9cfc5050ea6ca75beafb6e31ec2,dir/INSTALL
1609,343f3e1466662a92fa1804e2fc787e89474295f0ab086059e27ff86535dd1065,dir/README

__author__ = "anatoly techtonik <techtonik@gmail.com>"
__license__ = "Public Domain"
__version__ = "1.0"
"""


def filehash(filepath):
    blocksize = 64 * 1024
    sha = hashlib.sha256()
    with open(filepath, "rb") as fp:
        while True:
            data = fp.read(blocksize)
            if not data:
                break
            sha.update(data)
    return sha.hexdigest()


def hashdeep(root: str, ignore_regex: str = "") -> str:
    ignorer = re.compile(ignore_regex)

    lines = list()
    for root, dirs, files in os.walk(root):
        for fpath in [osp.join(root, f) for f in files]:
            if not ignorer.match(fpath):
                size = osp.getsize(fpath)
                sha = filehash(fpath)
                name = osp.relpath(fpath, root)
                lines.append("%s,%s,%s" % (size, sha, name))
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("root", default=".")
    parser.add_argument("--ignore-regex", default=".*__pycache__.*")
    args = parser.parse_args()

    print(hashdeep(args.root, args.ignore_regex))


if __name__ == "__main__":
    main()
