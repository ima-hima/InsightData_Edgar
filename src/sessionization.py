#! /usr/bin/env python

######################## Approximate pseudocode #########################

# ip_lookup -> dictionary of last query time -> set(ip)
# log_table -> dictionary of ip -> [original_query_time, num_documents]
# prev_time -> previous value of query time in log file; not value of last query
#
# interval  <- interval from file
# prev_time <- minimum time available in datetime
#
# for each line in log, check current_time
#     if current_time's changed
#         for log_item in ip_lookup for each where query_time in [prev_time + 1 - interval, current_time - interval]
#             print log_item, final_time = current_time - interval
#             delete log_table[log_item]
#             delete ip_lookup[current_time - interval]
#         change prev_time to current_time
#     if line[ip] exists in log_table (this means we're still inside interval)
#         update log_table[ip][num_documents] += 1
#         delete ip_lookup[current_time - interval + 1][ip]
#         add    ip_lookup[current_time][ip]
#     else (line[ip]) doesn't exist)
#         ip_lookup[current_time] = ip
#         log_table[ip][original_query_time] = current_time
#         log_table[ip][num_documents]       = 1
# for log_item in log_table, sorted by original_query_time:
#     print log_item, final_time = current_time

# TODOs:
# 1. Could be more modular
# 2. The data structure is a little complicated. Making it an object would be nice.
# 3. Not currently using Pythonic EAFP coding style vs. LBYL.
# 4. Dictionary comprehension and row printing could be a little prettier.

# test cases in my-tests:

# 1. time has expired but ip doesn't appear again
# 2. change in time > 1
# 3. queries are still active at end of log
# 4. other inactivity periods


from csv      import reader, writer, QUOTE_MINIMAL
from datetime import datetime, timedelta
from operator import itemgetter
from sys      import argv


def print_row(ip, log_table, logwriter):
    logwriter.writerow( [ ip
                        , log_table[0]
                        , log_table[1]
                        , (log_table[1] - log_table[0]).seconds + 1
                        , log_table[2]
                        ] )

def get_time(row):
    # This is fragile and will fail poorly: It relies completely on EDGAR being
    # properly formatted.
    # Also, I can ignore timezones, as none are given.
    return datetime.strptime(f'{row[1]} {row[2]}', "%Y-%m-%d %H:%M:%S")



def main():
    log_file          = argv[1]
    interval_filename = argv[2]
    output_filename   = argv[3]

    log_table = {} # This is a dict where I'll store info as
                   # {ip: [original_query_time, set_of_documents]}
                   # Lookups, insertions and deletions are all O(1), and I can remove a
                   # key:value pair without worrying about dealloc'ing the referenced tuple
                   # thanks to Python's garbage collection (thank you Python!).
                   # Because all Python values are boxed, updating the last query time
                   # doesn't force the entire tuple to be copied, so is also O(1).

    ip_lookup = {} # This is a dict of {datetime of last query: set of ip's}.
                   # Lookups insertions and deletions are all O(1).
                   # I'll use this to decide which queries have expired so that they
                   # can be removed more easily and atomically vs. having to do some
                   # of O(n) lookup.
                   # Tradeoff in memory efficiency is a constant factor (approx. 2).

    with open(interval_filename) as interval_file:
        interval = timedelta( seconds=int(interval_file.readline()) )

    # read a csv stream, capture these fields:
    # 0: IP
    # 1: date
    # 2: time
    # 4: cik
    # 5: accession
    # 6: extension
    #
    # 4, 5 & 6 will be concatenated to create a uid.
    # *** I don't actually use the uid. Queries to same doc still count as new queries. ***

    # csv iterates a file reading one line at a time, so memory footprint is small
    with open(output_filename, 'w') as output_stream:
        # logwriter should also only keep strings in memory till cache is cleared, so footprint is small.
        logwriter = writer(output_stream, delimiter=',', quotechar='"', quoting=QUOTE_MINIMAL)

        with open(log_file, newline='') as input_stream:
            next(input_stream) # throw away first line of input
            logline = reader(input_stream, delimiter=',', quotechar='"')
                # Could have used DictReader.
                # Difference is access via ref string or index. Using a reference aids legibility,
                # but this input file has different headers than EDGAR file and also
                # has "extension" spelled incorrectly. Sigh.

            # I need to get first line in log so that I can prime the following loop.
            # Specifically, prev_time must be first timestamp that appears in log.
            row = next(logline)
            prev_time = get_time(row)
            ip_lookup[prev_time] = {row[0]}
            log_table[row[0]] = [prev_time, prev_time, 1]
            ip = row[0]

            for row in logline:
                current_time = get_time(row)
                if current_time != prev_time: # Time has changed
                # If time has changed, check to see what's expired and delete it.
                    which_time = prev_time - interval # It may have changed by a number > interval.
                    delta      = timedelta(seconds=1)
                    while which_time < current_time - interval:
                        if which_time in ip_lookup:
                            stuff = {ip:log_table[ip] for ip in ip_lookup[which_time]}
                            for ip, value in sorted(stuff.items(),
                                          key=itemgetter(1)):
                                print_row(ip, value, logwriter)
                                del(log_table[ip])
                            del(ip_lookup[which_time]) # which_time is expired; remove entire set from ip_lookup.
                        which_time += delta
                    ip_lookup[current_time] = set()    # Create a new set for the new current_time.
                    prev_time = current_time
                ### We've finished dealing with expired queries ###

                ip = row[0]
                if ip in log_table:
                    log_table[ip][2] += 1
                    if ip not in ip_lookup[current_time]:
                    # So it's in the log_table, but for an earlier (but not expired) time.
                    # Update its log_table entry and move it to correct time in ip_lookup
                        ip_lookup[log_table[ip][1]].remove(ip)
                        ip_lookup[current_time].add(ip)
                        log_table[ip][1] = current_time
                else: # ip is new to us.
                    ip_lookup[current_time].add(ip)  # We know there's already a line in the lookup table for current_time.
                    log_table[ip] = [current_time, current_time, 1]

            ### End of log file. ###
            for ip, value in sorted(log_table.items(), key=itemgetter(1)):
                print_row(ip, value, logwriter)


if __name__ == "__main__": main()
