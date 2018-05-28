#! /usr/bin/env python

from csv      import reader, writer, QUOTE_MINIMAL
from datetime import datetime, timedelta
from operator import itemgetter

# def main():
log_table = {} # This is a dict where I'll store info as
         # {ip: [original_query_time, set_of_documents]}
         # Lookups, insertions and deletions are all O(1), and I can remove a
         # key:value pair without worrying about dealloc'ing the referenced tuple
         # thanks to Python's garbage collection (thank you Python!).
         # Because all Python values are boxed, updating the last query time
         # doesn't force the entire tuple to be copied, so is also O(1).
ip_lookup = {}
prev_time = datetime.min

dir_name = '../insight_testsuite/tests/test_1/'
with open(dir_name + 'input/inactivity_period.txt') as interval_file:
    interval = timedelta( seconds=int(interval_file.readline()) - 1 )

# read a csv stream, capture these fields:
# 0: IP
# 1: date
# 2: time
# 4: cik
# 5: accession
# 6: extension
#
# 4, 5 & 6 will be concatenated to create a uid. *** Do I actually need this? Queries to same doc still count as new queries.

# csv iterates a file reading one line at a time, so memory footprint is small
with open(dir_name + 'output/my_sessionization.txt', 'w') as output_stream:
    logwriter = writer(output_stream, delimiter=',', quotechar='"', quoting=QUOTE_MINIMAL)
    with open(dir_name + 'input/log.csv', newline='') as input_stream:
        next(input_stream)
        logline = reader(input_stream, delimiter=',', quotechar='"')
            # Could have used DictReader.
            # Difference is access via ref string or index. Using a reference aids legibility,
            # but this input file has different headers than EDGAR file and also
            # has "extension" spelled incorrectly. Sigh.
        row = next(logline)
        prev_time = datetime.strptime(f'{row[1]} {row[2]}', "%Y-%m-%d %H:%M:%S")
        ip_lookup[prev_time] = row[0]
        log_table[row[0]] = [prev_time, prev_time, 1]
        print(prev_time)
        for row in logline:
            current_time = datetime.strptime(f'{row[1]} {row[2]}', "%Y-%m-%d %H:%M:%S")
            if current_time != prev_time:
                which_time = current_time - interval
                delta      = timedelta(seconds=1)
                print('current', current_time, 'which', which_time, interval)
                while which_time < current_time - interval:
                    if which_time in ip_lookup:
                        # print(ip_lookup[which_time])
                        for ip in ip_lookup[which_time]:
                            # print(ip)
                            logwriter.writerow([ip, log_table[ip][0], current_time, (log_table[ip][1] - log_table[ip][0]).seconds + 1, log_table[ip][2]])
                            print('deleted')
                            print(ip, log_table[ip][0], current_time, (log_table[ip][1] - log_table[ip][0]).seconds + 1, log_table[ip][2])
                            del(log_table[ip])
                        del(ip_lookup[which_time])
                    which_time += delta
                    # print("Which:", which_time)
                    # print("Curr: ", current_time)
                ip_lookup[current_time] = set()
                prev_time = current_time
            ip = row[0]
            if ip in log_table:
                log_table[ip][2] += 1
                # del(ip_lookup[current_time - interval + timedelta(seconds=1)][ip])
                # print(ip_lookup[current_time])
                # print("adding:", ip)
                # ip_lookup[current_time].add(ip) It's already in there.
            else: # (line[ip]) doesn't exist)
                ip_lookup[current_time] = {ip}
                log_table[ip] = [current_time, current_time, 1]
        for ip in sorted(log_table, key=itemgetter(1)):
            logwriter.writerow([ip, log_table[ip][0], current_time, (log_table[ip][1] - log_table[ip][0]).seconds + 1, log_table[ip][2]])
            print(ip, log_table[ip][0], current_time, (log_table[ip][1] - log_table[ip][0]).seconds + 1, log_table[ip][2])
            # logwriter.writerow([ip, log_table[ip][0], current_time, (log_table[ip][1] - log_table[ip][0]).seconds + 1, log_table[ip][2]])



                # if current_time - log_table[ip][1] > interval: # in log_table but no longer active
                #     print("deleted", ip, log_table[ip][1], current_time, log_table[ip][1])
                #     logwriter.writerow([ip, log_table[ip][0], current_time, (current_time - log_table[ip][0]).seconds + 1, log_table[ip][2]])
                #     del log_table[ip]
                #     log_table[ip] = [current_time, current_time, 1]
                # else: # in the log_table and still active
                #     log_table[ip][1] = current_time
                #     print("inserted", ip, current_time, uid)
                #     log_table[ip][2] += 1  # I should put this in a try/except,
                #                      # but for now I'll assume it's safe to assume
                #                      # that the set already exists.


            # Declaring a variable here for legibility.
            uid = f'{row[4]}{row[5]}{row[6]}' # from my reading f'' should be slightly
                                              # faster than using '+', although
                                              # it probably doesn't matter for such
                                              # short strings.

            # This is fragile and will fail poorly: It relies completely on EDGAR being
            # properly formatted.
            # Also, I can ignore timezones, as none are given.
    #         current_time = datetime.strptime(f'{row[1]} {row[2]}', "%Y-%m-%d %H:%M:%S")
    # # print(current_time)
    #         if current_time != prev_time
    #         ip = row[0]
    #         if ip in log_table:
    #             if current_time - log_table[ip][1] > interval: # in log_table but no longer active
    #                 print("deleted", ip, log_table[ip][1], current_time, log_table[ip][1])
    #                 logwriter.writerow([ip, log_table[ip][0], current_time, (current_time - log_table[ip][0]).seconds + 1, log_table[ip][2]])
    #                 del log_table[ip]
    #                 log_table[ip] = [current_time, current_time, 1]
    #             else: # in the log_table and still active
    #                 log_table[ip][1] = current_time
    #                 print("inserted", ip, current_time, uid)
    #                 log_table[ip][2] += 1  # I should put this in a try/except,
    #                                  # but for now I'll assume it's safe to assume
    #                                  # that the set already exists.
    #         else: # need to insert new line in log_table
    #             log_table[ip] = [current_time, current_time, 1]
    #     for ip in sorted(log_table, key=itemgetter(1)):
    #         logwriter.writerow([ip, log_table[ip][0], current_time, (log_table[ip][1] - log_table[ip][0]).seconds + 1, log_table[ip][2]])


# New class:
# Be able to look up sessions by both IP and last query time.
# Store first query time, last query time, number of documents
# last query time is stored in
class LogQueue:
    #
    # last query time -> ip
    # ip ->
    def __init__():
        a = 5
