from __future__ import print_function
import nltk

class Tokens:

    def tokenize(self, s):
        '''
        Take a string and return a list of tokens split out from it
        with the nltk library
        '''

        # word_tokenize uses PunktSentenceTokenizer first, then
        # treebank_word_tokenizer on those so can get nested
        # lists.
        #return nltk.tokenize.word_tokenize(s)

        # this is just the treebank tokenizer
        t = nltk.tokenize.treebank.TreebankWordTokenizer()
        return t.tokenize(s)

if __name__ == "__main__":
    import sys
    fn = sys.argv[1]

    t = Tokens()
    with open(fn) as f:
        for l in f:
            tokens = t.tokenize(l)
            print(tokens)

    # In this case, the colons are not made in to separate tokens
    # when there is no space around them. Looses the possible 
    # s-v-p 
    #print(t.tokenize("Station = 3, Station=3 and VII-'65 Drawer:14. Cabinet: 167"))

