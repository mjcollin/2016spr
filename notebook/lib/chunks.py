import nltk
import json

class Chunks:

    # Right now we are doing no chunking of tokens to try
    # to build chunks and phrases
    def chunk(self, l):
        return l


    def extract_features(self, sentence, i):
        return {"tag": sentence[i]["tag"]}


    def load_training_data(self, fn):
        with open(fn) as f:
            self.training_data = []
            for l in f:
                self.training_data.append(json.loads(l))


    def train(self, fn):
        self.load_training_data(fn)
        #print(self.training_data)

        train_set = []
        for sentence in self.training_data:
            for i, pos_tagged in enumerate(sentence):
                print(pos_tagged)
                train_set.append( (self.extract_features(sentence, i), pos_tagged["iob"]) )

        print(train_set)
        #print(type(train_set))
        self.classifier = nltk.NaiveBayesClassifier.train(train_set)


    def predict(self, sentence):
        iob_tagged = []
        for i, pos_tagged in enumerate(sentence):
            iob = self.classifier.classify(self.extract_features(sentence, 1))
            dict = {"word": pos_tagged["word"], "tag": pos_tagged["tag"], "iob": iob}
            iob_tagged.append(dict)
            print(dict)
        return iob_tagged


    def assemble(self, iob_tagged):
        '''
        Re-format a list of iob tagged words into a pos tagged
        style where the words are concatenated together.
        '''
        words = []
        new_tagged = []
        for i, iob in enumerate(iob_tagged):
            if iob["iob"] in ["I-NP"]:
                words.append(iob)
            else:
                words = [iob]

            if (iob["iob"] not in ["I-NP"]) or i > len(iob_tagged): 
                phrase = " ".join([w["word"] for w in words])
                tag = "NP" if (len(words) > 1) else words[0]["tag"]
                new_tagged.append({"phrase": phrase, "tag": tag, "words": words})
                words = []

        return new_tagged


if __name__ == "__main__":
    from tokens import Tokens
    from pos_tags import PosTags
    import sys
    fn = sys.argv[1]

    t = Tokens()
    pos_tags = PosTags()
    chunker = Chunks()

    chunker.train("tests/chunk_training.json")

    with open(fn) as f:
        for l in f:
            tokens = t.tokenize(l)
            pos = pos_tags.tag(tokens)
            print(chunker.assemble(chunker.predict(pos)))
            #features = chunker.extract_features(pos, 0)
            #print(features)
            #chunks = chunker.chunk(pos)
            #print(chunks)
            #s = ""
            #for p in pos:
            #    s = s + p["word"] + " (" + p["tag"] + ") | "
            #print(s + "\n")
