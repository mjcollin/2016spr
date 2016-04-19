import sys
import json

# for a file that has manually been iob tagged, change the
# default tag of nouns to B-NP but only if the iob tag is O

fn = sys.argv[1]

with open(fn) as f:
    for l in f:
        row = json.loads(l)
        new_pos = []
        #print(len(row["pos"]))
        for pos in row:
            if pos["iob"] == "O" and pos["tag"].startswith("N"):
                new_iob = "B-NP"
            else:
                new_iob = "O"

            d = {"word": pos["word"], "tag": pos["tag"], "iob": new_iob}
            new_pos.append(d)
        print json.dumps(new_pos, sort_keys=True)
