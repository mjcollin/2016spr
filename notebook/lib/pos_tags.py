import nltk

class PosTags:

    def tag(self, t, as_dicts=True):
        '''
        With a list of tokens, mark their part of speech and return
        a list dicts (no native tuple type in dataframes it seems).
        '''
        pos = nltk.pos_tag(t)
        if as_dicts:
            return self.to_dicts(pos)
        else:
            return pos


    def to_dicts(self, pos):
        '''
        With a list of POS tag tuples, convert the tuples to dicts
        because Spark can't store tuples.
        '''
        retval = []
        for p in pos:
            retval.append({"word": p[0], "tag": p[1]})
        return retval


if __name__ == "__main__":
    from tokens import Tokens
    import sys
    fn = sys.argv[1]

    t = Tokens()
    pos_tags = PosTags()    
    with open(fn) as f:
        for l in f:
            tokens = t.tokenize(l)
            pos = pos_tags.tag(tokens)
            s = ""
            for p in pos:
                s = s + p["word"] + " (" + p["tag"] + ") | "
            print(s + "\n")
