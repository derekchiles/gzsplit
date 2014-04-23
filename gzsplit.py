#!/usr/bin/env python
#
# Copyright 2014, Derek Chiles. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
# http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
#
###############################################################################
#
# gzsplit - splits a gzip compressed text file into multiple gzip
# compressed text files
#
# Parameters:
#
#   infile         - a gzip compressed input file
#   outfile prefix - prefix for split and compressed output files
#   outfile count  - number of output files to create
#   
# The compressed input file is assumed to contain line-oriented text.
# 
# gzsplit will look at the size of the compressed input file and attempt
# to create compressed output files that are roughly equal in size, while
# still respecting the compressed text file's line boundaries.

import os
import sys
from gzip import GzipFile
from io import FileIO
from io import BufferedReader
from io import BufferedWriter

BUF_SIZE=8192

class ByteCountFileIO(FileIO):
    """Like FileIO, but keeps track of the number of bytes written."""
    
    def __init__(self, *args, **kwargs):
        super(ByteCountFileIO, self).__init__(*args, **kwargs)

        self.bytecount = 0

    def write(self, b):
        bytes_written = super(ByteCountFileIO, self).write(b)
        self.bytecount += bytes_written

        return bytes_written

def main(argv):
    usage = 'Usage: {} <infile> <outfile prefix> <outfile count>'

    if len(argv) != 4:
        print usage.format(argv[0])
        return 1

    in_file = argv[1]
    outfile_prefix = argv[2]
    outfile_count = int(argv[3])
    gzsplit(in_file, outfile_prefix, outfile_count)

    return 0

def gzsplit(infile, outfile_prefix, outfile_count):
    file_num = 1

    infile_size = os.path.getsize(infile)
    outfile_size = infile_size / outfile_count
    infile = GzipFile(infile, 'rb')

    while True:
        outfile_name = '{}-{}.gz'.format(outfile_prefix, file_num)
        eof = _gz_copy(infile, outfile_name, outfile_size)

        if eof:
            break

        file_num += 1

    infile.close()
        
def _gz_copy(infile, outfile_name, target_bytes):
    # raw file stream - gzip writes compressed bytes to it
    outfile_raw = ByteCountFileIO(outfile_name, 'w')

    # gzip file stream - we write uncompressed bytes to it
    outfile_gzip = GzipFile(
        filename = outfile_name,
        mode = 'wb',
        fileobj = outfile_raw)

    buf = ''
    eof = False

    # Get close to the target bytecount by doing efficient, buffered I/O.
    while True:
        if outfile_raw.bytecount >= target_bytes:
            break
        
        buf = infile.read(BUF_SIZE)
        if not buf:
            eof = True
            break

        outfile_gzip.write(buf)

    # We are at the target bytecount. We want to respect line boundaries
    # so write out the remainder of the current line.
    if not eof:
        line = infile.readline()
        if line:
            outfile_gzip.write(line)
        else:
            eof = True

    outfile_gzip.close()
    outfile_raw.close()

    return eof

if __name__ == '__main__':
    sys.exit(main(sys.argv))

