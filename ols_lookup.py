from __future__ import print_function
import sys
import json
import requests
from time import sleep
from datetime import datetime

# This script looks up terms from the OLS
# api: http://www.ebi.ac.uk/ols/beta/docs/api
#
# Load input and output JSON files. Read as
# sets of words. If input word is not in out
# set, then OWS api and append result to out.

class Ows:

    def __init__(self):
        self.url = "http://www.ebi.ac.uk/ols/beta/api"

    def get(self, endpoint, query):
        resp = requests.get("{0}/{1}?{2}".format(
                                          self.url,
                                          endpoint,
                                          query)
                           )
        return resp
 

def load_file(fn):
    rows = []
    try:
        with open(fn) as f:
            for l in f:
                rows.append(json.loads(l))
    except:
        pass

    return rows


def append_dict(fn, dict):
    try:
        with open(fn, "a") as f:
            f.write(json.dumps(dict))
            f.write("\n")
    except:
        pass


def word_list(rows):
    words = set()
    for r in rows:
        words.add(r["noun"])
    return words

if __name__ == "__main__":

    ows = Ows()

    in_fn = sys.argv[1]
    out_fn = sys.argv[2]

    in_words = word_list(load_file(in_fn))
    out_words = word_list(load_file(out_fn))

    toget_words = in_words - out_words

    #print(toget_words.pop())

    for word in toget_words:
        try:
            print(u"Looking for {0}".format(word))
            out = {"noun": word,
                   "time": datetime.now().isoformat()
                  }
            resp = ows.get("search", "q={0}&ontology=envo&groupField=iri&rows=1".format(word))
            if resp.json()["response"]["numFound"] > 0:
                out["response"] = resp.json()["response"]["docs"][0]
                out["term_id"] = out["response"]["id"]
                out["term_label"] = out["response"]["label"]
                #print(json.dumps(out, indent=2))
            else:
                out["response"] = {}
                out["term_id"] = ""
                out["term_label"] = ""

            append_dict(out_fn, out)

        except:
            print(u"Error getting {0}".format(word))

        sleep(0.5)
