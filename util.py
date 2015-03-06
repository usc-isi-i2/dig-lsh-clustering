__author__ = 'dipsy'
import os
from csvsort import csvsort

def write_tokens(tokens, sep):
    outStr = ""
    usep = ""
    for token in tokens:
        outStr = outStr + usep + str(token)
        usep = sep
    return outStr


def get_int_list(list1):
    ret = []
    for e in list1:
        if(len(e) > 0) and e != 'None':
            ret.append(int(e))
    return ret

def sort_csv_file(filename, column, delim):
    #This a hack to fix a bug with sorting multiple files using this csvsort library.
    TMP_DIR = '.csvsort.%d' % os.getpid()
    if not os.path.exists(TMP_DIR):
        os.mkdir(TMP_DIR)

    csvsort(filename, [column], delimiter=delim)


def binary_search_in_file(filename, matchvalue, key=lambda val: val):
    max_line_len = 2 ** 12

    start = pos = 0
    end = os.path.getsize(filename)

    fptr = open(filename, 'rb')

        # Limit the number of times we binary search.

    for rpt in xrange(50):

        last = pos
        pos = start + ((end - start) / 2)
        fptr.seek(pos)

        # Move the cursor to a newline boundary.

        fptr.readline()

        line = fptr.readline()
        linevalue = key(line)

        if linevalue == matchvalue or pos == last:

            # Seek back until we no longer have a match.

            while True:
                fptr.seek(-max_line_len, 1)
                fptr.readline()
                if matchvalue != key(fptr.readline()):
                    break

           # Seek forward to the first match.

            for rpt in xrange(max_line_len):
                line = fptr.readline()
                linevalue = key(line)
                if matchvalue == linevalue:
                    break
            else:
                # No match was found.

                return []

            results = []

            while linevalue == matchvalue:
                results.append(line)
                line = fptr.readline()
                linevalue = key(line)

            return results
        elif linevalue < matchvalue:
            start = fptr.tell()
        else:
            assert linevalue > matchvalue
            end = fptr.tell()
    else:
        raise RuntimeError('binary search failed')

    fptr.close()

