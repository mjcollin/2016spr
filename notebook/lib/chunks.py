class Chunks:

    # Right now we are doing no chunking of tokens to try
    # to build chunks and phrases
    def chunk(self, l):
        return l


    # FIXME: this function is a cut-paste, need to pick a
    # format for storing chunks
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
    from pos_tags import PosTags
    import sys
    fn = sys.argv[1]

    t = Tokens()
    pos_tags = PosTags()
    chunker = Chunks()

    with open(fn) as f:
        for l in f:
            tokens = t.tokenize(l)
            pos = pos_tags.tag(tokens, as_dicts=False)
            chunks = chunker.chunk(pos)
            print(chunks)
            #s = ""
            #for p in pos:
            #    s = s + p["word"] + " (" + p["tag"] + ") | "
            #print(s + "\n")
