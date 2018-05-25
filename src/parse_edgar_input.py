#! /usr/bin/env python

from csv      import reader
from datetime import datetime, timedelta

# def main():
log = {} # This is a dict where I'll store info as ip:(lastQueryTime,set_of_documents)
         # Lookups, insertions and deletions are all O(1), and I can remove a key:value pair without
         # worrying about dealloc'ing the referenced tuple thanks to Python's garbage collection (thank you Python!).
         # Because all Python values are boxed, updating the last query time doesn't force the entire
         # tuple to be copied, so is also O(1).

dir_name = '../insight_testsuite/tests/test_1/'
with open(dir_name + 'input/inactivity_period.txt') as interval_file:
    interval = int(interval_file.readline())

# read a csv stream, capture these fields:
# 0: IP
# 1: date
# 2: time
# 4: cik
# 5: accession
# 6: extension
#
# 4, 5 & 6 need to be concated to create a uid

# csv iterates a file reading one line at a time, so memory footprint is small
with open(dir_name + 'input/log.csv', newline='') as input_stream:
    next(input_stream)
    logline = reader(input_stream, delimiter=',', quotechar='"')
    for row in logline:
        # print(row[4], row[5], row[6])
        uid = f'{row[4]}{row[5]}{row[6]}' # from my reading f'' should be slightly faster than using '+', although
                                          # it probably doesn't matter for such short strings.
                                          # declaring a variable for legibility
        # This is fragile and could fail poorly. It relies heavily on EDGAR being properly formatted.
        # Also, I can ignore timezones, as none are given.
        timestamp = datetime.strptime(f'{row[1]} {row[2]}', "%Y-%m-%d %H:%M:%S")
# print(timestamp)
        ip = row[0]
        if ip in log:
            if timestamp - log[ip][0]  > timedelta(seconds=2):
                print("deleted", ip, timestamp, log[ip][0])
                del log[ip]
            else:
                log[ip][0] = timestamp
                print("inserted", ip, timestamp, uid)
                log[ip][1].add(uid)  # I should put this in a try/except, but for now I'll assume it's safe to assume
                                     # that the set already exists.
        else:
            to_insert = {uid}
            log[ip] = [timestamp, to_insert]
    for ip in log:
        print(ip, log[ip][0], "\n\t", log[ip][1], '\n')



# logfile = open("../insight_testsuite/tests/test_1/input/log.csv")

# for line in (logfile):
#     print(line)

