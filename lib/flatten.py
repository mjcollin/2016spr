from __future__ import print_function
import json


class flatten():

    def __init__(self):
        try:
            # Just for testing, read from file in future
            meta_json = '{"barcodevalue":{"type":"string","fieldName":"barcodevalue"}}'
            self.meta = json.loads(meta_json)
        except Exception as e:
            raise ValueError("Unable to read in the metadata structure for records", e)


    def flatten_record(self, j):
        """
        Take the JSON representation of a record as returned by Elastic Search and
        flatten it to a dict with safely named keys based on the path to the value
        in the JSON. Uses the indexing information in the meta/records endpoint of
        the search API to know what the keys are. Returns a complete dict with all
        keys from the enpoints list and NULLs for keys that were unset in the JSON
        """

        for k in self.meta.keys():
	    print(k)

        return j


if __name__ == "__main__":
    f = flatten()

    with open("../test/data/es_search_acer.json") as es_file:
        for l in es_file:
            j = json.loads(l)
            print(f.flatten_record("asdf"))
