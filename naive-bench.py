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

import random, time, optparse, humanize
import socket, sys, os, re, math, hashlib
import functools

from os import system
from tqdm import tqdm
from tqdm import trange
from functools import partial
from itertools import repeat
from multiprocessing import Pool, freeze_support, Lock, Process, Manager

#
# Global constants
#
kibybytes = ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB']
kilobytes = ['KB', 'MB', 'GB', 'TB', 'PB', 'EB']

process_manager = Manager()

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

__test_data_dir = "naive-bench-data"



def get_random_file_size(filesize, dev):
    """
    Get randomized file size based on average 'filesize' and deviation range.
    """
    min_range = (1.0-dev)*filesize
    max_range = (1.0+dev)*filesize
    return int( (max_range-min_range)*random.random() + min_range )

def get_random_data(size):
    #
    # Open the random device for writing test files
    #
    randfile = open("/dev/urandom", "rb")
    randdata = randfile.read(size)
    randfile.close()
    return randdata

#
# This function parses the file sizes supporting both conventions 
# (i.e. KiB and KB)
#
def parse_file_size(file_size_string):

    file_size = float('nan')
    #
    # First check if the number is in bytes without suffix
    # if it's not try to match a known suffix
    #
    try:
        file_size = int(file_size_string)
        return file_size
    except ValueError:
        parse_result = re.split(r'([\.\d]+)', file_size_string)
        try:
            file_size = float(parse_result[1])
            file_size_suffix = -1
            if parse_result[2] in kibybytes:
                file_size *= math.pow(1024, (kibybytes.index(parse_result[2])+1))
                return file_size
            elif parse_result[2] in kilobytes:
                file_size *= math.pow(1000, (kilobytes.index(parse_result[2])+1))
                return file_size
            else:
                return float('nan')
        except ValueError:
            return float('nan')
        return False



#
# Global lock for progress bar functionality
#
tqdm_lock = Lock()

def init_child_process(write_lock):
    """
    Provide tqdm with the lock from the parent app.
    This is necessary on Windows to avoid racing conditions.
    """
    #tqdm.set_lock(write_lock)


def run_benchmark(file_create_benchmark, \
                  filecount, threadcount, deviation, blocksize, \
                  threads_results, threads_progress_messages):
    """
    This a generic function for running naive benchmarks
    """

    start_barrier = Manager().Barrier(threadcount+1)

    #
    # Prepapre a list of arguments for each benchmark task
    #
    file_create_benchmark_args = []
    for tidx in range(threadcount):

        r = range(int(tidx*(filecount/threadcount)), \
                  int((tidx+1)*(filecount/threadcount)-1))

        file_create_benchmark_args.append(\
            (tidx, r, filesize, deviation, blocksize, __test_data_dir, \
               threads_results, threads_progress_messages, start_barrier))
        threads_results[tidx] = 0
        threads_progress_messages[tidx] = "Starting task "+str(tidx)

    #
    # Create the process pool and run the benchmark
    #
    progress_bars = []
    for i in range(threadcount):
        child = Process(target=file_create_benchmark, \
                        args=file_create_benchmark_args[i])
        child.start()
        threads.append(child)

    #
    # Wait for all benchmark tasks to initialize
    #
    start_barrier.wait()

    start_time = time.time()
    #
    # Wait for the threads to complete and printout the progress every 
    # 0.5 second
    #
    while any(thread.is_alive() for thread in threads):
        time.sleep(0.5)
        for i in range(threadcount):
            print(threads_progress_messages[i])
        for i in range(threadcount):
            sys.stdout.write("\x1b[A")

    for i in range(threadcount):
        print(threads_progress_messages[i])

    real_execution_time = time.time() - start_time

    return real_execution_time


def file_create_benchmark(task_id, file_ids, filesize, deviation, \
                          blocksize, test_data_dir, \
                          thread_results, thread_progress_messages, \
                          start_barrier):
    """
    Task which creates a set of test files and measures 
    """

    total_written_bytes = 0

    #
    # Generate random file sizes and calculate total size for this task
    #
    random_file_sizes = \
                  [get_random_file_size(filesize, deviation) for i in file_ids]
    total_size_to_write = sum(random_file_sizes)

    thread_progress_messages[task_id] = \
            "Task # " + str(task_id) + ": Written " \
          + humanize.naturalsize(0) \
          + " of " + humanize.naturalsize(total_size_to_write) \
          + " | " + "?" \
          + "/s"

    randdata = get_random_data(blocksize)

    #
    # Initialize the tqdm progress bar
    #
    start_time = time.time()
    start_barrier.wait()
    for i in range(len(file_ids)):
        #
        # Create random size file
        #
        rand_size = random_file_sizes[i]
        outfile = open(test_data_dir + "/" + str(file_ids[i]), "wb")
        #
        # Rewrite random device to the output file in 'blocksize' blocks
        #
        file_written_bytes = 0
        while(file_written_bytes + blocksize < rand_size):
            block_written_bytes = outfile.write(randdata)
            file_written_bytes += block_written_bytes
            total_written_bytes += block_written_bytes
            # 
            # Format progress message
            # 
            thread_progress_messages[task_id] = \
                    "Task # " + str(task_id) + ": Written " \
                  + humanize.naturalsize(total_written_bytes) \
                  + " of " + humanize.naturalsize(total_size_to_write) \
                  + " | " + humanize.naturalsize(total_written_bytes/(time.time()-start_time)) \
                  + "/s       "

        #
        # Write remainder of the file
        #
        block_written_bytes = \
                    outfile.write(randdata[0:rand_size - file_written_bytes])
        total_written_bytes += block_written_bytes

    end_time = time.time() - start_time
    thread_results[task_id] = (total_written_bytes, end_time)



def file_write_benchmark(task_id, file_ids, filesize, deviation, \
                          blocksize, test_data_dir, \
                          thread_results, thread_progress_messages, \
                          start_barrier):

    total_written_bytes = 0

    #
    # Generate random file sizes and calculate total size for this task
    #
    random_file_sizes = \
                  [get_random_file_size(filesize, deviation) for i in file_ids]
    total_size_to_write = sum(random_file_sizes)

    thread_progress_messages[task_id] = \
            "Task # " + str(task_id) + ": Written " \
          + humanize.naturalsize(0) \
          + " of " + humanize.naturalsize(total_size_to_write) \
          + " | " + "?" \
          + "/s"

    randdata = get_random_data(blocksize)

    #
    # Initialize the tqdm progress bar
    #
    start_time = time.time()
    start_barrier.wait()
    for i in range(len(file_ids)):
        #
        # Create random size file
        #
        rand_size = random_file_sizes[i]
        outfile = open(test_data_dir + "/" + str(file_ids[i]), "wb")
        #
        # Rewrite random device to the output file in 'blocksize' blocks
        #
        file_written_bytes = 0
        while(file_written_bytes + blocksize < rand_size):
            block_written_bytes = outfile.write(randdata)
            file_written_bytes += block_written_bytes
            total_written_bytes += block_written_bytes
            # 
            # Format progress message
            # 
            thread_progress_messages[task_id] = \
                    "Task # " + str(task_id) + ": Written " \
                  + humanize.naturalsize(total_written_bytes) \
                  + " of " + humanize.naturalsize(total_size_to_write) \
                  + " | " + humanize.naturalsize(total_written_bytes/(time.time()-start_time)) \
                  + "/s       "

        #
        # Write remainder of the file
        #
        block_written_bytes = \
                    outfile.write(randdata[0:rand_size - file_written_bytes])
        total_written_bytes += block_written_bytes

    end_time = time.time() - start_time
    thread_results[task_id] = (total_written_bytes, end_time)


def file_linear_read_benchmark(task_id, file_ids, filesize, deviation, \
                               blocksize, test_data_dir, \
                               thread_results, thread_progress_messages, \
                               start_barrier):

    total_read_bytes = 0

    #
    # Calculate the size of files to read
    #
    file_sizes = {}
    for f in file_ids:
        file_sizes[f] = os.path.getsize(test_data_dir+"/"+str(f))

    total_size_to_read = sum(file_sizes.values())

    thread_progress_messages[task_id] = \
            "Task # " + str(task_id) + ": Read " \
          + humanize.naturalsize(0) \
          + " of " + humanize.naturalsize(total_size_to_read) \
          + " | " + "?" \
          + "/s"

    #
    # Initialize the tqdm progress bar
    #
    outfile = open("/dev/null", "wb")
    start_time = time.time()
    start_barrier.wait()
    for i in range(len(file_ids)):
        #
        # Open file
        #
        infile = open(test_data_dir + "/" + str(file_ids[i]), "rb")

        #
        # Read the file in blocks
        #
        file_read_bytes = 0
        
        while(file_read_bytes + blocksize < file_sizes[file_ids[i]]):
            block_read_bytes = outfile.write(infile.read(blocksize))
            file_read_bytes += block_read_bytes
            total_read_bytes += block_read_bytes
            # 
            # Format progress message
            # 
            thread_progress_messages[task_id] = \
                    "Task # " + str(task_id) + ": Read " \
                  + humanize.naturalsize(total_read_bytes) \
                  + " of " + humanize.naturalsize(total_size_to_read) \
                  + " | " + humanize.naturalsize(total_read_bytes/(time.time()-start_time)) \
                  + "/s       "

        #
        # Write remainder of the file
        #
        block_read_bytes = \
            outfile.write(infile.read(file_sizes[file_ids[i]]-file_read_bytes))
        total_read_bytes += block_read_bytes

    outfile.close()
    end_time = time.time() - start_time
    thread_results[task_id] = (total_read_bytes, end_time)


def file_random_read_benchmark(task_id, file_ids, filesize, deviation, \
                               blocksize, test_data_dir, \
                               thread_results, thread_progress_messages, \
                               start_barrier):

    total_read_bytes = 0

    #
    # Calculate the size of files to read
    #
    file_sizes = {}
    for f in file_ids:
        file_sizes[f] = os.path.getsize(test_data_dir+"/"+str(f))

    total_size_to_read = sum(file_sizes.values())

    thread_progress_messages[task_id] = \
            "Task # " + str(task_id) + ": Read " \
          + humanize.naturalsize(0) \
          + " of " + humanize.naturalsize(total_size_to_read) \
          + " | " + "?" \
          + "/s"

    #
    # Initialize the tqdm progress bar
    #
    outfile = open("/dev/null", "wb")
    start_time = time.time()
    start_barrier.wait()
    for i in range(len(file_ids)):
        #
        # Open file
        #
        infile = open(test_data_dir + "/" + str(file_ids[i]), "rb")
        infile_size = file_sizes[file_ids[i]]

        #
        # Read the file in blocks
        #
        file_read_bytes = 0
        
        while(file_read_bytes + blocksize < infile_size):
            infile.seek(int(random.random()*(int(infile_size/blocksize)-1)), 0)            
            block_read_bytes = outfile.write(infile.read(blocksize))
            file_read_bytes += block_read_bytes
            total_read_bytes += block_read_bytes
            # 
            # Format progress message
            # 
            thread_progress_messages[task_id] = \
                    "Task # " + str(task_id) + ": Read " \
                  + humanize.naturalsize(total_read_bytes) \
                  + " of " + humanize.naturalsize(total_size_to_read) \
                  + " | " + humanize.naturalsize(total_read_bytes/(time.time()-start_time)) \
                  + "/s       "

        #
        # Write remainder of the file
        #
        block_read_bytes = \
            outfile.write(infile.read(file_sizes[file_ids[i]]-file_read_bytes))
        total_read_bytes += block_read_bytes

    outfile.close()
    end_time = time.time() - start_time
    thread_results[task_id] = (total_read_bytes, end_time)



if __name__ == '__main__':
    #
    # Parse command line options
    #
    parser = optparse.OptionParser()

    parser.add_option('-f', '--filecount',
        action="store", dest="filecount", type='int',
        help="Number of files to create", default=100)

    parser.add_option('-s', '--filesize', type='string',
        action="store", dest="filesize",
        help="""Average created file size. The file sizes will be random
        in the range (0.5*filesize, 1.5*filesize].""", 
        default=1024*1024)

    parser.add_option('-b', '--blocksize', type='string',
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

    parser.add_option('-d', '--deviation', type='float',
        action="store", dest="deviation",
        help="""Generate the files with random size in range 
    ((1.0-deviation)*filesize, (1.0+deviation)*filesize].""", 
        default=0.0)

    parser.add_option('-t', '--thread-count', type='int',
        action="store", dest="threadcount",
        help="""Number of threads to execute for each test.""", 
        default=4)

    #
    # Parse the command line
    #
    options, args = parser.parse_args()

    filesize = parse_file_size(options.filesize)
    filecount = int(options.filecount)
    blocksize = parse_file_size(options.blocksize)
    deviation = options.deviation
    threadcount = options.threadcount

    if math.isnan(filesize):
        print("Invalid filesize - exiting.", file=sys.stderr)  
        sys.exit(2)
    else:
        filesize = int(filesize)

    if math.isnan(blocksize):
        print("Invalid blocksize - exiting.", file=sys.stderr)  
        sys.exit(2)
    else:
        blocksize = int(blocksize)

    if deviation < 0.0 or deviation > 0.9:
        print("Deviation must be in range [0.0, 0.9] - exiting.", file=sys.stderr)  
        sys.exit(2)

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

    #
    # Calculate available disk space on the current volume
    #
    st = os.statvfs(os.getcwd())
    available_disk_space = st.f_bavail * st.f_frsize

    #
    # Printout basic benchmark parameters
    #
    print("------------------------------", file=sys.stderr)
    print('Starting test')
    print("  Number of files: ", filecount, file=sys.stderr)
    print('  Average file size: ', humanize.naturalsize(filesize), file=sys.stderr)
    print('  Maximum disk space needed: ', \
          humanize.naturalsize(filesize * filecount * (1.0+deviation)), 
          file=sys.stderr)
    print('  Available disk space: ', \
          humanize.naturalsize(available_disk_space), file=sys.stderr)
    print('  Number of parallel threads:', \
          threadcount, file=sys.stderr)
    print('------------------------------')


    #
    # Check available disk space for test
    #
    if (filesize * filecount * (1.0+deviation)) > available_disk_space \
                                                 and not options.force:
        print("Not enough disk space to perform test - exiting.", file=sys.stderr)
        sys.exit(1)

    #
    # Check conflicting options
    #
    if options.readonly and options.writeonly:
        print("Cannot perform readonly and writeonly test - exiting.", 
              file=sys.stderr)  
        sys.exit(2)

    if options.filecount < 1:
        print("Cannot perform test with no files - exiting.", file=sys.stderr)  
        sys.exit(2)

    #
    # Initialize time variables
    #
    create_files_time = float('NaN')
    overwrite_files_time = float('NaN')
    linear_read_time = float('NaN')
    random_read_time = float('NaN')
    delete_time = float('NaN')


    print("\n\nCreating test folder 'naive-bench-data':", file=sys.stderr)
    #
    # Cleanup old test data and 
    #
    system("rm -rf naive-bench-data")
    starttime = time.time()
    system("mkdir naive-bench-data")
    endtime = time.time() - starttime
    print("Created test folder in " + str(endtime) + "s", file=sys.stderr)

    system(flush)



    ##########
    #
    # Start file creation benchmark
    #
    #
    threads = []
    threads_results = process_manager.dict()
    threads_progress_messages = process_manager.dict()
    print("\nCreating test files:", file=sys.stderr)
    
    create_files_time = run_benchmark(file_create_benchmark, \
                               filecount, threadcount, deviation, \
                               blocksize, threads_results, \
                               threads_progress_messages)

    #
    # Calculate total benchmark size and time
    #
    total_size = sum(s[0] for s in threads_results.values())

    print("")
    print("Created " + str(filecount) + " files of total size " \
          + str(humanize.naturalsize(total_size)) + " in " \
          + str(create_files_time) + "s", file=sys.stderr)
    print("Create throughput: " \
          + str(humanize.naturalsize(total_size/create_files_time) ) + "/s")
    print("")
    system(flush)


    ##########
    #
    # Start file overwrite benchmark
    #
    #
    threads = []
    threads_results = process_manager.dict()
    threads_progress_messages = process_manager.dict()
    print("\nOverwriting files:", file=sys.stderr)
    
    overwrite_files_time = run_benchmark(file_write_benchmark, \
                                         filecount, threadcount, deviation, \
                                         blocksize, threads_results, \
                                         threads_progress_messages)

    #
    # Calculate total benchmark size and time
    #
    total_size = sum(s[0] for s in threads_results.values())

    print("")
    print("Overwritten " + str(filecount) + " files with total size " \
          + str(humanize.naturalsize(total_size)) + " in " \
          + str(overwrite_files_time) + "s", file=sys.stderr)
    print("Overwrite throughput: " \
          + str(humanize.naturalsize(total_size/overwrite_files_time) ) + "/s")
    print("")
    system(flush)


    ##########
    #
    # Start linear read benchmark
    #
    #
    threads = []
    threads_results = process_manager.dict()
    threads_progress_messages = process_manager.dict()
    print("\nReading files:", file=sys.stderr)
    
    linear_read_time = run_benchmark(file_linear_read_benchmark, \
                                         filecount, threadcount, deviation, \
                                         blocksize, threads_results, \
                                         threads_progress_messages)

    #
    # Calculate total benchmark size and time
    #
    total_size = sum(s[0] for s in threads_results.values())

    print("")
    print("Read " + str(filecount) + " files with total size " \
          + str(humanize.naturalsize(total_size)) + " in " \
          + str(linear_read_time) + "s", file=sys.stderr)
    print("Linear read throughput: " \
          + str(humanize.naturalsize(total_size/linear_read_time) ) + "/s")
    print("")
    system(flush)


    ##########
    #
    # Start random read benchmark
    #
    #
    threads = []
    threads_results = process_manager.dict()
    threads_progress_messages = process_manager.dict()
    print("\nReading files:", file=sys.stderr)
    
    random_read_time = run_benchmark(file_random_read_benchmark, \
                                         filecount, threadcount, deviation, \
                                         blocksize, threads_results, \
                                         threads_progress_messages)

    #
    # Calculate total benchmark size and time
    #
    total_size = sum(s[0] for s in threads_results.values())

    print("")
    print("Read " + str(filecount) + " files with total size " \
          + str(humanize.naturalsize(total_size)) + " in " \
          + str(random_read_time) + "s", file=sys.stderr)
    print("Random read throughput: " \
          + str(humanize.naturalsize(total_size/random_read_time) ) + "/s")
    print("")
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


