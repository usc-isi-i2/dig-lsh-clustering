__author__ = 'dipsy'
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

def compute_list_similarity(list1, list2):
    similarity = float(len(set(list1) & set(list2)))/float(len(set(list1)))
    return similarity

def sort_csv_file(filename, columnArr, delim):
    #This a hack to fix a bug with sorting multiple files using this csvsort library.
    TMP_DIR = '.csvsort.%d' % os.getpid()
    if not os.path.exists(TMP_DIR):
        os.mkdir(TMP_DIR)
    from csvsort import csvsort
    if os.stat(filename).st_size > 0:
        csvsort(filename, columnArr, delimiter=delim, has_header=False)
        try:
            for the_file in os.listdir(TMP_DIR):
                file_path = os.path.join(TMP_DIR, the_file)
                try:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                except:
                    pass
            os.rmdir(TMP_DIR)
        except OSError:
            pass

class Searcher:
    def __init__(self, filename):
        self.f = open(filename, 'rb')
        self.f.seek(0,2)
        self.length = self.f.tell()

    def readToEndOfLineAfterPos(self, p):
        while p >= 0:
            self.f.seek(p)
            if self.f.read(1) == '\n': break
            p -= 1
        if p < 0: self.f.seek(0)
        return p

    def seekBackToFindStr(self, p, string, key=lambda val: val):
        delta = 100
        prev_p = -1
        while p >= 0:
            p = p - delta
            p = self.readToEndOfLineAfterPos(p)
            line = (self.f.readline())
            #print "seek got:", line, ":", string, ":", p
            if key(line) != string:
                p = prev_p
                self.readToEndOfLineAfterPos(p)
                break
            prev_p = p
        if p < 0: self.f.seek(0)
        return p

    def find(self, string,  key=lambda val: val):
        low = 0
        high = self.length
        while low < high:
            mid = (low+high)//2
            p = mid
            self.readToEndOfLineAfterPos(p)
            line = self.f.readline()
            keyVal = key(line)
            #print '--', low, high, mid, keyVal
            if keyVal == string:
                low = self.seekBackToFindStr(mid, string, key)
                break

            if keyVal < string:
                low = mid+1
            else:
                high = mid

        self.readToEndOfLineAfterPos(low)

        result = [ ]
        while True:
            line = self.f.readline()
            keyVal = key(line)
            if not line or not keyVal == string: break
            if line[-1:] == '\n': line = line[:-1]
            #print "Append: " + line + "\n"
            result.append(line)
        return result
