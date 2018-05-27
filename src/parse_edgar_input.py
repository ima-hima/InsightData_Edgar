#! /usr/bin/env python

from csv      import reader, writer, QUOTE_MINIMAL
from datetime import datetime, timedelta

# def main():
log = {} # This is a dict where I'll store info as
         # {ip: [original_query_time, mostRecent_query_time, set_of_documents]}
         # Lookups, insertions and deletions are all O(1), and I can remove a
         # key:value pair without worrying about dealloc'ing the referenced tuple
         # thanks to Python's garbage collection (thank you Python!).
         # Because all Python values are boxed, updating the last query time
         # doesn't force the entire tuple to be copied, so is also O(1).

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
with open(dir_name + 'output/my_sessionization.txt', 'w') as output_stream:
    logwriter = writer(output_stream, delimiter=',', quotechar='"', quoting=QUOTE_MINIMAL)
    with open(dir_name + 'input/log.csv', newline='') as input_stream:
        next(input_stream)
        logline = reader(input_stream, delimiter=',', quotechar='"') # Could have used DictReader
                                                                     # difference is access via string
                                                                     # or index. String is quicker
                                                                     # but aids legibility
        print(logline)
        for row in logline:
            # print(row[4], row[5], row[6])
            # Declaring a variable here for legibility.
            uid = f'{row[4]}{row[5]}{row[6]}' # from my reading f'' should be slightly
                                              # faster than using '+', although
                                              # it probably doesn't matter for such
                                              # short strings.

            # This is fragile and could fail poorly. It relies completely on EDGAR being
            # properly formatted.
            # Also, I can ignore timezones, as none are given.
            timestamp = datetime.strptime(f'{row[1]} {row[2]}', "%Y-%m-%d %H:%M:%S")
    # print(timestamp)
            ip = row[0]
            if ip in log:
                if timestamp - log[ip][1] > timedelta(seconds=2):
                    print("deleted", ip, log[ip][1], timestamp, log[ip][1])
                    logwriter.writerow([ip, log[ip][0], timestamp, timestamp - log[ip][0], len(log[ip][2])])
                    del log[ip]
                else:
                    log[ip][0] = timestamp
                    log[ip][1] = timestamp
                    print("inserted", ip, timestamp, uid)
                    log[ip][2].add(uid)  # I should put this in a try/except,
                                         # but for now I'll assume it's safe to assume
                                         # that the set already exists.
            else: # need to insert new line in log
                to_insert = {uid}
                log[ip] = [timestamp, timestamp, to_insert]
        for ip in log:
            logwriter.writerow([ip, log[ip][0], timestamp, timestamp - log[ip][0], len(log[ip][2])])




# logfile = open("../insight_testsuite/tests/test_1/input/log.csv")

# for line in (logfile):
#     print(line)

