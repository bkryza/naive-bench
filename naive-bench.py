#!/usr/bin/env python
# -*- coding: utf-8 -*-


import random, time, optparse, humanize
from os import system
from sys import platform


#
# Parse command line options
#
parser = optparse.OptionParser()

parser.add_option('-f', '--filecount',
    action="store", dest="filecount",
    help="Number of files to create", default=1000)

parser.add_option('-s', '--filesize',
    action="store", dest="filesize",
    help="Number of files to create", default=1024*1024)

options, args = parser.parse_args()

filesize = int(options.filesize)
filecount = int(options.filecount)

print '------------------------------'
print 'Starting test'
print '  Number of files: ', filecount
print '  Average file size: ', humanize.naturalsize(filesize)
print '  Total disk space used: ', humanize.naturalsize(filesize * filecount * 1.5)
print '------------------------------'




#
# Define command for flushing cache
#
flush = ""
if platform == "linux" or platform == "linux2":
    flush = "sudo su -c 'sync ; echo 3 > /proc/sys/vm/drop_caches'"
elif platform == "darwin":
    flush = "sync"
else:
    print "Unsupported file system - exiting."
    sys.exit(1)

#
# Open the random device for writing test files
#
randfile = open("/dev/urandom", "r")

print "\n\nCreating test folder:"
starttime = time.time()
system("rm -rf test && mkdir test")
print time.time() - starttime
system(flush)

print "\nCreating files:"
starttime = time.time()
for i in xrange(filecount):
    rand = randfile.read(int(filesize * 0.5 + filesize * random.random()))
    outfile = open("test/" + unicode(i), "w")
    outfile.write(rand)
print time.time() - starttime
system(flush)

print "\nRewrite files:"
starttime = time.time()
for i in xrange(int(filecount / 10)):
    rand = randfile.read(int(filesize * 0.5 + filesize * random.random()))
    outfile = open("test/" + unicode(int(random.random() * filecount)), "w")
    outfile.write(rand)
print time.time() - starttime
system(flush)

print "\nRead linear:"
starttime = time.time()
outfile = open("/dev/null", "w")
for i in xrange(int(filecount / 10)):
    infile = open("test/" + unicode(i), "r")
    outfile.write(infile.read());
print time.time() - starttime
system(flush)

print "\nRead random:"
starttime = time.time()
outfile = open("/dev/null", "w")
for i in xrange(int(filecount / 10)):
    infile = open("test/" + unicode(int(random.random() * filecount)), "r")
    outfile.write(infile.read());
print time.time() - starttime
system(flush)

print "\nDelete all files:"
starttime = time.time()
system("rm -rf test")
print time.time() - starttime
system(flush)

