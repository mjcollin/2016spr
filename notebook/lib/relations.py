class Relations:

    def find(self, pos):
        '''
        From tagged parts of speech, find relationships
        between terms as triples of the form s-v-p
        '''

        # Algo: scan for implies_relation, emit back, current
        # and forward. Pop emited and skipped.
        # no additional checks permits the assumed subject of
        # this specimen eg "in alcohol"
        rels = []
        last_p = False
        rel_p = False

        for p in pos:

            if rel_p != False and self.can_be_subj(p):
                if last_p != False:
                    rels.append({"s": last_p, "v": rel_p, "p": p})
                else:
                    rels.append({"v": rel_p, "p": p})
                last_p = False
                rel_p = False

            else:
                if rel_p == False and self.implies_relation(p):
                    rel_p = p
                else:
                    if self.can_be_subj(p): last_p = p


        return rels


    def can_be_subj(self, token):
        return \
            token["tag"].startswith("N") or \
            token["tag"] == "CD"


    def implies_relation(self, token):
        '''
        Does this token imply a relationship?
        '''
        return \
            token["word"] == ":" or \
            token["tag"].startswith("V") or \
            token["tag"].startswith("IN") or \
            token["word"] == "="

    def to_str(self, rels):
        s = ""
        for r in rels:
            if r.get("s", False):
                s = s + "{0} ({1}) - ".format(r["s"]["word"], 
                                              r["s"]["tag"])
            s = s + "{0} ({1}) - {2} ({3}) | ".format(r["v"]["word"],
                                                      r["v"]["tag"],
                                                      r["p"]["word"],
                                                      r["p"]["tag"])
        return s



if __name__ == "__main__":
    from tokens import Tokens
    from pos_tags import PosTags
    import sys
    fn = sys.argv[1]

    t = Tokens()
    pos_tags = PosTags()
    relations = Relations()

    with open(fn) as f:
        for l in f:
            tokens = t.tokenize(l)
            pos = pos_tags.tag(tokens)
            rels = relations.find(pos)
            print(relations.to_str(rels))
