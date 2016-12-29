#!/usr/bin/env python
# -*- coding: utf-8 -*-



import random, time, optparse, humanize, socket, sys, os
from os import system

#
# Parse command line options
#
parser = optparse.OptionParser()

parser.add_option('-f', '--filecount',
    action="store", dest="filecount", type='int',
    help="Number of files to create", default=1000)

parser.add_option('-s', '--filesize', type='int',
    action="store", dest="filesize",
    help="Number of files to create", default=1024*1024)

parser.add_option('-n', '--name',
    action="store", dest="name",
    help="Name of storage", default=socket.gethostname())

parser.add_option('-c', '--csv',
    action="store_true", dest="csv",
    help="Generat CSV row", default=False)

parser.add_option('-H', '--header',
    action="store_false", dest="header", 
    help="Generated csv header", default=True)



options, args = parser.parse_args()

filesize = int(options.filesize)
filecount = int(options.filecount)

print >> sys.stderr, '------------------------------'
print >> sys.stderr, 'Starting test'
print >> sys.stderr, '  Number of files: ', filecount
print >> sys.stderr, '  Average file size: ', humanize.naturalsize(filesize)
print >> sys.stderr, '  Maximum disk space needed: ', humanize.naturalsize(filesize * filecount * 1.5)
print >> sys.stderr, '------------------------------'



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
create_files_time = 0
overwrite_files_time = 0
linear_read_time = 0
random_read_time = 0
delete_time = 0



#
# Define command for flushing cache
#
flush = ""
if sys.platform == "linux" or sys.platform == "linux2":
    flush = "sudo su -c 'sync ; echo 3 > /proc/sys/vm/drop_caches'"
elif sys.platform == "darwin":
    flush = "sync"
else:
    print "Unsupported file system - exiting."
    sys.exit(1)

#
# Open the random device for writing test files
#
randfile = open("/dev/urandom", "r")

print >> sys.stderr, "\n\nCreating test folder:"
system("rm -rf naive-bench-data")
starttime = time.time()
system("mkdir naive-bench-data")
endtime = time.time() - starttime
print >> sys.stderr,endtime

system(flush)

#
# Create files with random content
#
print >> sys.stderr, "\nCreating files:"
starttime = time.time()
for i in xrange(filecount):
    rand = randfile.read(int(filesize * 0.5 + filesize * random.random()))
    outfile = open("naive-bench-data/" + unicode(i), "w")
    outfile.write(rand)
create_files_time = time.time() - starttime
print >> sys.stderr, create_files_time
system(flush)

#
# Overwrite all files using /dev/random
#
print >> sys.stderr, "\nRewrite files:"
starttime = time.time()
for i in xrange(int(filecount / 10)):
    rand = randfile.read(int(filesize * 0.5 + filesize * random.random()))
    outfile = open("naive-bench-data/" + unicode(int(random.random() * filecount)), "w")
    outfile.write(rand)
overwrite_files_time = time.time() - starttime
print >> sys.stderr, overwrite_files_time
system(flush)

#
# Read entire randomly selected files (1/10th of the population)
#
print >> sys.stderr, "\nRead linear:"
starttime = time.time()
outfile = open("/dev/null", "w")
for i in xrange(int(filecount / 10)):
    #infile = open("naive-bench-data/" + unicode(i), "r")
    infile = open("naive-bench-data/" + unicode(int(random.random() * filecount)), "r")
    outfile.write(infile.read());
linear_read_time = time.time() - starttime
print >> sys.stderr, linear_read_time
system(flush)

#
# Perform 10 random reads on 1/10th of files
# Each read reads 1024 bytes
#
print >> sys.stderr, "\nRead random:"
read_block_size = 1024
starttime = time.time()
outfile = open("/dev/null", "w")
for i in xrange(int(filecount / 10)):
    infile_path = "naive-bench-data/" + unicode(int(random.random() * filecount))
    infile_size = os.path.getsize(infile_path)
    if infile_size < 2*read_block_size+1:
        continue;
    infile = open(infile_path, "r")
    for i in xrange(1, 10):
        infile.seek((random.random()*int((infile_size/read_block_size)-1)))
        outfile.write(infile.read(read_block_size));
random_read_time = time.time() - starttime
print >> sys.stderr, random_read_time
system(flush)

#
# Delete the entire test folder
#
print >> sys.stderr, "\nDelete all files:"
starttime = time.time()
system("rm -rf naive-bench-data")
delete_time = time.time() - starttime
print >> sys.stderr, delete_time
system(flush)

#
# Print CSV on stdout
#
if options.csv:
    if options.header:
        print storage_name_label + ";" + number_files_label + ";" \
              + average_file_size_label + ";" + create_files_label + ";" \
              + overwrite_files_label + ";" + linear_read_label + ";" \
              + random_read_label + ";" + delete_label

    print options.name + ";" + str(filecount) + ';' + str(filesize) + ';' \
          + str(create_files_time) + ';' + str(overwrite_files_time) + ';' \
          + str(linear_read_time) + ';' + str(random_read_time) + ';' \
          + str(delete_time)


