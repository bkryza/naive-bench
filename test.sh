#!/usr/bin/env bats


@test "Blocksize must not be larger than filesize" {
  run ./naive-bench.py -P --filecount 10 --filesize 20MB --blocksize 10GB  -t 5 2>&1
  [ $status -eq 2 ]
  [[ $output == *"Blocksize must not be larger than filesize - exiting"* ]]
}

@test "File count must be multiple of thread count" {
  run ./naive-bench.py -P --filecount 10 --filesize 20MB --blocksize 100KB  -t 3 2>&1
  [ $status -eq 2 ]
  [[ $output == *"Total file count must be a multiple of thread count - exiting."* ]]
}

@test "CSV should be produced on the output" {
  run ./naive-bench.py -P --filecount 10 --filesize 20MB --blocksize 100KB  -t 2 -c
  [ $status -eq 0 ]
  [[ $output == *"STORAGE NAME;FILE COUNT;AVERAGE FILE SIZE [b];CREATE TIME [s];CREATE SIZE [b];WRITE TIME [s];WRITE SIZE [b];LINEAR READ TIME [s];LINEAR READ SIZE [b];RANDOM READ TIME [s];RANDOM READ SIZE [b];DELETE"* ]]
}

@test "Test with -H option should not contain CSV header" {
  run ./naive-bench.py -P --filecount 10 --filesize 20MB --blocksize 100KB  -t 2 2>&1
  [ $status -eq 0 ]
  [[ $output != *"STORAGE NAME;FILE COUNT;AVERAGE FILE SIZE [b];"* ]]
  [ ! -d "naive-bench-data" ]
}

@test "Data files should be kept with option k" {
  run ./naive-bench.py -P --filecount 10 --filesize 20MB --blocksize 100KB  -t 2 -k
  [ $status -eq 0 ]
  [ -d "naive-bench-data" ]
  [ "$(ls -1 naive-bench-data | wc -l)" -eq "10" ]
}

@test "Created file sizes should be equal to specified size" {
  run ./naive-bench.py -P --filecount 10 --filesize 20MB --blocksize 90KB  -t 2 -k
  [ $status -eq 0 ]
  [ -d "naive-bench-data" ]
  [ "$(ls -la naive-bench-data | grep 20000000 | wc -l)" -eq "10" ]
}





