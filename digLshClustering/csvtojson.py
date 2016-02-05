#!/usr/bin/env python

import sys
import codecs
import json


def parse_args():
    global inputFilename
    global outputFilename
    global separator
    global headerStartIndex
    global dataStartIndex
    global groupByColumn
    global header
    global pretty_print

    for arg_idx, arg in enumerate(sys.argv):
        if arg == "--help":
            usage()
            exit(1)
        if arg == "--input":
            inputFilename = sys.argv[arg_idx+1]
            continue
        if arg == "--output":
            outputFilename = sys.argv[arg_idx+1]
            continue
        if arg == "--separator":
            separator = sys.argv[arg_idx+1]
            continue
        if arg == "--headerStartIndex":
            headerStartIndex = int(sys.argv[arg_idx+1])
            continue
        if arg == "--dataStartIndex":
            dataStartIndex = int(sys.argv[arg_idx+1])
            continue
        if arg == "--groupByColumn":
            groupByColumn = int(sys.argv[arg_idx+1])
            continue
        if arg == "--header":
            header = sys.argv[arg_idx+1].split(",")
            continue
        if arg == "--prettyPrint":
            if sys.argv[arg_idx+1].lower() == "true":
                pretty_print = True


def die():
    print "Please input the required parameters"
    usage()
    exit(1)


def usage():
    print "\nUsage: csvtojson.py --input <input filename> --output <output filename> [--separator <sep=\\t>] " \
          "[--headerStartIndex 1] [--dataStartIndex 2] [--groupByColumn 0] [--header <comma separated header names>] " \
          "[--prettyPrint True|False, default=False]"
    print "Note: All indices are 1 based, 0 means do not use"
    print "Note: If groupByColumn is used, the input should be sorted by the grouped column. The utility only groups" \
          " consecutively similar values\n"


def generate_empty_header(row):
    global separator
    header_arr = []
    index = 1
    for part in row.split(separator):
        header_arr.append("Column" + str(index))
        index += 1
    return header_arr


def parse_header(row):
    global separator
    header_arr = []
    for part in row.split(separator):
        header_arr.append(part)
    return header_arr


def generate_json(headings, row):
    global separator
    global groupByColumn

    index = 0
    row_json = {}
    row_parts = row.split(separator)

    if groupByColumn != 0:
        heading = headings[groupByColumn-1]
        row_json[heading] = row_parts[groupByColumn-1]
        row_json["matches"] = []
        part_json = {}
        row_json["matches"].append(part_json)

    for part in row_parts:
        heading = headings[index]
        if groupByColumn == 0:
            row_json[heading] = part
        else:
            if index != groupByColumn-1:
                part_json = row_json["matches"][0]
                part_json[heading] = part

        index += 1

    return row_json


def group_json_by_column(group, row, column_name):
    if group is None:
        return row, None
    if group[column_name] == row[column_name]:
        group["matches"].extend(row["matches"])
        return group, None
    else:
        return row, group

inputFilename = None
dataStartIndex = 2
headerStartIndex = 1
separator = "\t"
outputFilename = None
groupByColumn = 0
header = None
pretty_print = False

parse_args()
if inputFilename is None or outputFilename is None:
    die()

inputFile = codecs.open(inputFilename,'r',"utf-8")
outputFile = codecs.open(outputFilename,'w','utf-8')

lineNumber = 1
outputFile.write("[")
sep = ""
group_json = None
prev_line_json = None

for line in inputFile:
    line = line.strip()
    if header is None and lineNumber == headerStartIndex:
        header = parse_header(line)

    if lineNumber >= dataStartIndex:
        if header is None:
            header = generate_empty_header(line)
        line_json = generate_json(header, line)

        if groupByColumn == 0:
            outputFile.write(sep + "\n" + json.dumps(line_json))
            sep = ","
        else:
            group_json, line_json = group_json_by_column(group_json, line_json, header[groupByColumn-1])
            if line_json is not None:
                compact_json = {"source":line_json[header[groupByColumn-1]],"candidates":line_json["matches"]}
                if pretty_print is True:
                    outputFile.write(sep + "\n" + json.dumps(compact_json, indent=2, separators=(',', ': ')))
                else:
                    outputFile.write(sep + "\n" + json.dumps(compact_json))
                sep = ","



outputFile.write("\n]")
outputFile.close()
inputFile.close()
