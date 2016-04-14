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
    t = Tokens()
    tokens = t.tokenize("Hello, my name is Mace Windoo")
    print(tokens)

    # In this case, the colons are not made in to separate tokens
    # when there is no space around them. Looses the possible 
    # s-v-p 
    print(t.tokenize("Station = 3, Station=3 and VII-'65 Drawer:14. Cabinet: 167"))

