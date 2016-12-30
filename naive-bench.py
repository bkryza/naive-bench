#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# The MIT License (MIT)
# Copyright (c) 2016 Bartosz Kryza <bkryza at gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.
#

import random, time, optparse, humanize, socket, sys, os
from os import system



#
# Parse command line options
#
parser = optparse.OptionParser()

parser.add_option('-f', '--filecount',
    action="store", dest="filecount", type='int',
    help="Number of files to create", default=100)

parser.add_option('-s', '--filesize', type='int',
    action="store", dest="filesize",
    help="""Average created file size. The file sizes will be random
    in the range (0.5*filesize, 1.5*filesize].""", 
    default=1024*1024)

parser.add_option('-b', '--blocksize', type='int',
    action="store", dest="blocksize",
    help="""Size of data block for random read test.""", default=1024)

parser.add_option('-n', '--name',
    action="store", dest="name",
    help="""Name of storage which identifies the performed test. 
Defaults to hostname.""", 
    default=socket.gethostname())

parser.add_option('-c', '--csv',
    action="store_true", dest="csv",
    help="Generate CSV output.", default=False)

parser.add_option('-H', '--no-header',
    action="store_true", dest="skipheader",
    help="Skip CSV header.", default=False)

parser.add_option('-r', '--read-only',
    action="store_true", dest="readonly",
    help="""This test will only perform read tests. 
It assumes that the current folder contains 'naive-bench-data' folder 
with test files uniformly numbered in the specified range.""", 
    default=False)

parser.add_option('-w', '--write-only',
    action="store_true", dest="writeonly",
    help="""This test will only perform write tests.
This option can be used to create data on storage for peforming
    remote read tests.""", default=False)

parser.add_option('-k', '--keep',
    action="store_true", dest="keep",
    help="""Keep the files after running the test.""", default=False)

parser.add_option('-F', '--force',
    action="store_true", dest="force",
    help="""Run the test even when the available storage size is too small.""", 
    default=False)


options, args = parser.parse_args()

filesize = int(options.filesize)
filecount = int(options.filecount)

#
# Define command for flushing cache
#
flush = ""
if sys.platform == "linux" or sys.platform == "linux2":
    flush = "sudo sh -c 'sync ; echo 3 > /proc/sys/vm/drop_caches'"
elif sys.platform == "darwin":
    flush = "sudo sh -c 'sync; purge'"
else:
    print ( sys.platform, " platform is not supported - exiting." )
    sys.exit(1)

st = os.statvfs(os.getcwd())
available_disk_space = st.f_bavail * st.f_frsize

print("------------------------------", file=sys.stderr)
print('Starting test')
print("  Number of files: ", filecount, file=sys.stderr)
print('  Average file size: ', humanize.naturalsize(filesize), file=sys.stderr)
print('  Maximum disk space needed: ', \
                     humanize.naturalsize(filesize * filecount * 1.5), file=sys.stderr)
print('  Available disk space: ', \
                     humanize.naturalsize(available_disk_space), file=sys.stderr)                   
print('------------------------------')


#
# Check available disk space for test
#
if (filesize * filecount * 1.5) > available_disk_space and not options.force:
    print("Not enough disk space to perform test - exiting.", file=sys.stderr)
    sys.exit(1)

#
# Check conflicting options
#
if options.readonly and options.writeonly:
    print("Cannot perform readonly and writeonly test - exiting.", file=sys.stderr)  
    sys.exit(2)

if options.filecount < 1:
    print("Cannot perform test with no files - exiting.", file=sys.stderr)  
    sys.exit(2)

#
# CSV file row field order
# - STORAGE NAME
# - NUMBER OF FILES
# - AVERAGE FILE SIZE
# - FILE CREATION TIME
# - FILE WRITING TIME
# - LINEAR READS TIME
# - RANDOM READS TIME
# - DELETE TIME
#

#
# Initialize CSV column labels
#
storage_name_label = "STORAGE NAME"
number_files_label = "FILE COUNT"
average_file_size_label = "AVERAGE FILE SIZE"
create_files_label = "CREATE"
overwrite_files_label = "WRITE"
linear_read_label = "LINEAR_READ"
random_read_label = "RANDOM_READ"
delete_label = "DELETE"

#
# Initialize time variables
#
create_files_time = float('NaN')
overwrite_files_time = float('NaN')
linear_read_time = float('NaN')
random_read_time = float('NaN')
delete_time = float('NaN')


#
# Open the random device for writing test files
#
randfile = open("/dev/urandom", "rb")

if not options.readonly:
    print("\n\nCreating test folder 'naive-bench-data':", file=sys.stderr)
    system("rm -rf naive-bench-data")
    starttime = time.time()
    system("mkdir naive-bench-data")
    endtime = time.time() - starttime
    print("Created test folder in " + str(endtime) + "s", file=sys.stderr)

system(flush)

#
# Create files with random content
#
if not options.readonly:
    print("\nCreating test files:", file=sys.stderr)
    starttime = time.time()
    total_size = 0
    for i in range(filecount):
        rand_size = int(filesize*0.5 + filesize*random.random())
        outfile = open("naive-bench-data/" + str(i), "wb")

        #
        # Rewrite random device to the output file in 'blocksize' blocks
        #
        written_bytes = 0
        while(written_bytes + options.blocksize < rand_size):
            written_bytes += outfile.write(randfile.read(options.blocksize))

        #
        # Write remainder of the file
        #
        written_bytes += outfile.write(randfile.read(rand_size - written_bytes))

        total_size += written_bytes

    create_files_time = time.time() - starttime
    print("Created " + str(filecount) + " files of total size " \
             + str(humanize.naturalsize(total_size)) + " in " \
             + str(create_files_time) + "s", file=sys.stderr)

    system(flush)


#
# Overwrite 1/10th of files using /dev/random
#
print("\nPerform write test:", file=sys.stderr)
starttime = time.time()
total_size = 0
for i in range(filecount if filecount<10 else int(filecount / 10)):
    rand_size = int(filesize*0.5 + filesize*random.random())
    outfile = open("naive-bench-data/" \
              + str(int(random.random() * filecount)), "wb")
    
    #
    # Write the blocks of size 'blocksize'
    #
    written_bytes = 0
    while(written_bytes + options.blocksize < rand_size):
        written_bytes += outfile.write(randfile.read(options.blocksize))

    #
    # Write remainder of the file
    #
    written_bytes += outfile.write(randfile.read(rand_size - written_bytes))

    total_size += written_bytes

overwrite_files_time = time.time() - starttime
print("Written " + str(humanize.naturalsize(total_size)) \
         + " in " + str(overwrite_files_time) + "s", file=sys.stderr)

system(flush)


#
# Read entire randomly selected files (1/10th of the population)
#
used_files = []
total_read_size = 0
if not options.writeonly:
    print("\nPerforming linear read test:", file=sys.stderr)
    starttime = time.time()
    outfile = open("/dev/null", "wb")
    for i in range(filecount if filecount<10 else int(filecount / 10)):
        file_id = int(random.random() * filecount)
        used_files.append(file_id)
        infile = open("naive-bench-data/" \
                 + str(file_id), "rb")
        written_bytes += outfile.write(infile.read(options.blocksize));
        total_read_size += written_bytes
        while(written_bytes == options.blocksize):
            written_bytes = outfile.write(randfile.read(options.blocksize))
            total_read_size += written_bytes
    linear_read_time = time.time() - starttime
    print("Read " + str(humanize.naturalsize(total_read_size)) + " in " \
          + str(linear_read_time) + "s", file=sys.stderr)

    system(flush)

#
# Perform 10 random reads on 1/10th of files
# Each read reads options.blocksize bytes
#
#
print("\nPerforming random read test:", file=sys.stderr)
read_block_size = options.blocksize
starttime = time.time()
outfile = open("/dev/null", "wb")
total_read_size = 0
for i in range(filecount if filecount<10 else int(filecount / 10)):
    #
    # Try to randomly select a file that has not been yet used
    #
    file_id = int(random.random() * filecount)
    while(not file_id in used_files):
        file_id = int(random.random() * filecount)

    used_files.append(file_id)
    infile_path = "naive-bench-data/" + str(file_id)
    #
    # Check the test file size and make sure it's not too small
    #
    infile_size = os.path.getsize(infile_path)
    if infile_size < 2*read_block_size+1:
        continue;
    #
    # Open the file for reading, select 10 random 'blocksize' blocks and read 
    # them
    # 
    infile = open(infile_path, "rb")
    for i in range(0, int(infile_size/read_block_size)-1):
        infile.seek(int(random.random()*(int(infile_size/read_block_size)-1)), \
                    0)
        total_read_size += outfile.write(infile.read(read_block_size));
random_read_time = time.time() - starttime
print("Read " + str(humanize.naturalsize(total_read_size)) + " in " \
          + str(random_read_time) + "s", file=sys.stderr)

system(flush)

#
# Delete the entire test folder
#
if not options.keep:
    print("\nDeleting all files:", file=sys.stderr)
    starttime = time.time()
    system("rm -rf naive-bench-data")
    delete_time = time.time() - starttime
    print("Deleted all files in " + str(delete_time) + "s", file=sys.stderr)
    system(flush)

#
# Print CSV on stdout
#
if options.csv:
    if not options.skipheader:
        print(storage_name_label + ";" + number_files_label + ";" \
              + average_file_size_label + ";" + create_files_label + ";" \
              + overwrite_files_label + ";" + linear_read_label + ";" \
              + random_read_label + ";" + delete_label)

    print(options.name + ";" + str(filecount) + ';' + str(filesize) + ';' \
          + str(create_files_time) + ';' + str(overwrite_files_time) + ';' \
          + str(linear_read_time) + ';' + str(random_read_time) + ';' \
          + str(delete_time))


