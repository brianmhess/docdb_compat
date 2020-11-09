#!/usr/bin/python3
import sys
import yaml
from mtools.util import logevent
import csv

def all_keys(x):
    k = []
    if (dict == type(x)):
        for kk in x.keys():
            k.append(kk)
            k = k + all_keys(x[kk])
    elif (list == type(x)):
        for vv in x:
            k = k + all_keys(vv)
    return k

def dollar_keys(x):
    return list(set([k for k in all_keys(x) if k.startswith('$')]))

keywords = {}
def load_keywords(fname):
    global keywords
    with open(fname) as csv_file:
        reader = csv.DictReader(csv_file, delimiter=',')
        for k in reader.fieldnames[1:]:
            if ('Command' == k):
                continue
            keywords[k] = {}
        for row in reader:
            for k in keywords.keys():
                keywords[k][row['Command']] = row[k]
    #print(keywords.keys())
    return keywords

def check_keys(query, usage_map, ver):
    unsupported = False
    for k in dollar_keys(query):
        if ('No' == keywords[ver][k]):
            #print(f'{k} unsupported')
            usage_map[k] = usage_map.get(k, 0) + 1
            unsupported = True
    return unsupported

def process_aggregate(le, usage_map, ver):
    retval = {}
    command = yaml.load(" ".join(le.split_tokens[le.split_tokens.index("command:")+2:le.split_tokens.index("planSummary:")]))
    #print(command)
    p_usage_map = {}
    for p in command["pipeline"]:
        check_keys(p, p_usage_map, ver)
    for k in p_usage_map.keys():
        usage_map[k] = usage_map.get(k, 0) + 1
    retval = {"unsupported": (0 < len(p_usage_map.keys())), "unsupported_keys": list(p_usage_map.keys()), "logevent": le}
    return retval

def process_query(le, usage_map, ver): 
    retval = {}
    p_usage_map = {}
    check_keys(yaml.load(le.actual_query), p_usage_map, ver)
    for k in p_usage_map.keys():
        usage_map[k] = usage_map.get(k, 0) + 1
    retval = {"unsupported": (0 < len(p_usage_map.keys())), "unsupported_keys": list(p_usage_map.keys()), "logevent": le}
    return retval

def process_line(line, usage_map, ver):
    retval = {}
    le = logevent.LogEvent(line)
    if ('COMMAND' == le. component):
        if le.command in ['find', 'update']:
            #print("Processing find...")
            retval = process_query(le, usage_map, ver)
        if le.command in ['aggregate']:
            #print("Processing aggregate...")
            retval = process_aggregate(le, usage_map, ver)
    elif ('QUERY' == le.component):
        #print("Processing query...")
        retval = process_query(le, usage_map, ver)
    
    return retval

def process_log_file(fname, unsupported_fname, ver): 
    unsupported_file = open(unsupported_fname, "w")
    usage_map = {}
    line_ct = 0
    unsupported_ct = 0
    with open(fname) as log_file:
        for line in log_file:
            line_ct += 1
            pl = process_line(line, usage_map, ver)
            if (pl["unsupported"]):
                unsupported_file.write(pl["logevent"].line_str)
                unsupported_file.write("\n")
                unsupported_ct += 1
    unsupported_file.close()
    
    print(f'Results:\n\t{unsupported_ct} out of {line_ct} unsupported')
    print(f'Unsuported operators (and number of queries used)')
    #sorted_usage_map = {k: v for k, v in sorted(x.items(), key=lambda item: item[1])}
    for k,v in sorted(usage_map.items(), key=lambda x: (-x[1],x[0])):
        print(f'\t{k:20}  {v}')
    #for k in usage_map.keys():
    #    print(f'\t{k}\t{usage_map[k]}')
    print(f'Log lines of unsupported operators logged here: {unsupported_fname}')

def print_usage():
    print("Usage: compat.py <version> <input_file> <output_file>")
    print("  version : 3.6 or 4.0")
    print("  input_file: location of MongoDB log file")
    print("  output_file: location to write log lines that correspond to unsupported operators")

def main(args):
    if (3 != len(args)):
        print_usage()
        sys.exit()
    ver = args[0]
    infname = args[1]
    outfname = args[2]
    load_keywords('./docdb_compat/dollar_ver_20201109.csv')
    process_log_file(infname, outfname, ver)
    


if __name__ == '__main__':
    main(sys.argv[1:])