import sys
import json

# This is nice but really should be defaulting to B-NP for 
# all nouns?

fn = sys.argv[1]

with open(fn) as f:
    for l in f:
        row = json.loads(l)
        new_pos = []
        #print(len(row["pos"]))
        for pos in row["pos"]:
            d = {"word": pos["word"], "tag": pos["tag"], "iob": "O"}
            new_pos.append(d)
        print json.dumps(new_pos, sort_keys=True)
