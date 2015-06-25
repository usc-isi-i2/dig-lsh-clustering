import json
import sys
import codecs


def find_text(uri,inputJsonFile):
	file = open(inputJsonFile,"r")
	data = json.load(file)

	hits = data["hits"]["hits"]
	for row in hits:
		if row.has_key("_source"):
			source = row["_source"]
			uri_input = source["uri"]
			if(uri == uri_input):
				if source.has_key("hasBodyPart"):
					bp = source["hasBodyPart"]
					if bp.has_key("text"):
						body_text = bp["text"]
						return body_text	
					
						
def die():
    print "Please input the required parameters"
    print "Usage: combine-body-text.py [input json having body text] [clusters json] [output filename] "
    exit(1)

if len(sys.argv) < 4:
    die()
				
inputJsonFile = sys.argv[1]
outputJsonFile = sys.argv[2]
newOutputJsonFile = sys.argv[3]


file = open(outputJsonFile,"r")
data_no_body = json.load(file)
number_clusters = data_no_body["numClusters"]
numHashes = data_no_body["lshSettings"]["numHashes"]
numItemsInBand = data_no_body["lshSettings"]["numItemsInBand"]

wFile = open(newOutputJsonFile, "w")
wFile.write("{\n")
wFile.write("\"numClusters\":" + str(number_clusters) + ",\n")
wFile.write("\"lshSettings\":\n")
wFile.write("\t{\n")
wFile.write("\t\"numHashes\":" + str(numHashes) + ",\n")
wFile.write("\t\"numItemsInBand\":" + str(numItemsInBand) + "\n")
wFile.write("\t},\n")
wFile.write("\"clusters\":\n")

wFile.write("\t[\n")
sep1 = ""


clusters = data_no_body["clusters"]

for cluster in clusters:
    wFile.write(sep1)
    wFile.write("\t{\"cluster\": \n")
    wFile.write("\t\t[\n")
    urilist = cluster["cluster"]
    sep2 = ""
    for uri in urilist:
        wFile.write(sep2)
        wFile.write("\t\t{\n")
        text = find_text(uri,inputJsonFile)
        '''uri = uri + ": " + text'''
        str=json.dumps(uri,indent=16)
        str = str[1:len(str)-1]
        wFile.write("\t\t" + "\"uri\" :" +"\"" + str + "\",")
        wFile.write("\n")
        str = json.dumps(text,indent=16)
        str = str[1:len(str)-1]
        wFile.write("\t\t" + "\"body\" :" + "\"" + str +"\"" )
        wFile.write("\n\t\t}")
        sep2=",\n"

    wFile.write("\n\t\t]")
    wFile.write("\n\t}")
    sep1=",\n"

wFile.write("\n\t]\n")
wFile.write("}\n")











			

