#! /usr/bin/env python

from csv      import reader, writer, QUOTE_MINIMAL
from datetime import datetime, timedelta
from operator import itemgetter

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
    interval = timedelta( seconds=int(interval_file.readline()) )

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
        logline = reader(input_stream, delimiter=',', quotechar='"')
            # Could have used DictReader.
            # Difference is access via ref string or index. Reference aids legibility
            # but this input file has different headers than EDGAR file, and also
            # has "extension" spelled incorrectly. Sigh.
        print(logline)
        for row in logline:
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
                if timestamp - log[ip][1] > interval: # in log but no longer active
                    print("deleted", ip, log[ip][1], timestamp, log[ip][1])
                    logwriter.writerow([ip, log[ip][0], timestamp, (timestamp - log[ip][0]).seconds + 1, log[ip][2]])
                    del log[ip]
                    log[ip] = [timestamp, timestamp, 1]
                else: # in the log and still active
                    log[ip][1] = timestamp
                    print("inserted", ip, timestamp, uid)
                    log[ip][2] += 1  # I should put this in a try/except,
                                     # but for now I'll assume it's safe to assume
                                     # that the set already exists.
            else: # need to insert new line in log
                log[ip] = [timestamp, timestamp, 1]
        for ip in sorted(log, key=itemgetter(1)):
            logwriter.writerow([ip, log[ip][0], timestamp, (log[ip][1] - log[ip][0]).seconds + 1, log[ip][2]])




# logfile = open("../insight_testsuite/tests/test_1/input/log.csv")

# for line in (logfile):
#     print(line)


# New process:
# 1. Log interval == interval - 1, because inclusive DONE
# 2. Open input doc                                  DONE
# 3. Read csv using Python csv reader                DONE
# 4. For each line, note current query time. While time is constant either
#    a) add new IP with original query time == latest query == current timestamp,
#       no. docs = 1
#    b) updating existing IPs with latest query == current timestamp, no. docs += 1
# 5. If time changes print and then delete any IPs that were logged `interval` ago.


# New class:
# Be able to look up sessions by both IP and last query time.
# Store first query time, last query time, number of documents
class LogQueue:
    # last query time -> ip
    # ip ->
    def __init__():
        a = 5
