import urllib

# Input: a line splited by tab
def parse_line(line):
    c1, c2, count = '', '', ''
    # 2015-01: prev_id, curr_id, count, prev_title, curr_title
    if len(line) == 5:
        prev_id, curr_id, count, c1, c2 = line
    # 2015-02: prev_id, curre_id, count, prev_title, curr_title, type
    elif len(line) == 6:
        prev_id, curr_id, count, c1, c2, type = line
    # 2016: prev_title, current_title, count, count 
    elif len(line) == 4:
        c1, c2, type, count = line

    if count == 'n':
        return None

    if count == '':
        return None
    else:
        count = int(count)
    return urllib.quote_plus(c1), urllib.quote_plus(c2), count
