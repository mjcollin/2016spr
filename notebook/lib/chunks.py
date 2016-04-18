import nltk
import json

class Chunks():

    # Right now we are doing no chunking of tokens to try
    # to build chunks and phrases
    def chunk(self, l):
        return l


    def extract_features(self, sentence, i):
        features = {}
        features["tag"] = tag = sentence[i]["tag"]
        features["prior_tag"] = "START" if (i == 0) else sentence[i-1]["tag"]
        features["next_tag"] = "END" if (i == len(sentence) - 1) else sentence[i+1]["tag"]
        features["starts_cap"] = True if sentence[i]["word"] == sentence[i]["word"].upper() else False
        return features


    def load_training_data(self, fn):
        training_data = []
        with open(fn) as f:
            for l in f:
                training_data.append(json.loads(l))
        return training_data


    def train(self, training_data):
        #self.load_training_data(fn)
        #print(self.training_data)

        train_set = []
        for sentence in training_data:
            for i, pos_tagged in enumerate(sentence):
                #print(pos_tagged)
                train_set.append( (self.extract_features(sentence, i), pos_tagged["iob"]) )

        #print(train_set)
        #print(type(train_set))
        self.classifier = nltk.NaiveBayesClassifier.train(train_set)


    def tag(self, sentence):
        iob_tagged = []
        for i, pos_tagged in enumerate(sentence):
            iob = self.classifier.classify(self.extract_features(sentence, 1))
            dict = {"word": pos_tagged["word"], "tag": pos_tagged["tag"], "iob": iob}
            iob_tagged.append(dict)
            #print(dict)
        return iob_tagged


    def evaluate(self, test_data):
        # positive and negative relative to whether a noun phrase was identified
        tp = 0
        fp = 0
        tn = 0
        fn = 0

        for sentence in test_data:
            iob_sentence = self.tag(sentence)
            for i, iob in enumerate(iob_sentence):
                print(iob)
                print(sentence[i])

                iob_in = iob["iob"] in ["B-NP", "I-NP"]
                pos_in = sentence[i]["iob"] in ["B-NP", "I-NP"]
                print(iob_in)
                print(pos_in)

                if iob_in and pos_in:
                    tp += 1
                elif iob_in and not pos_in:
                    fp += 1
                elif not iob_in and not pos_in:
                    tn += 1
                elif not iob_in and pos_in:
                    fn += 1

        n = tp + fp + tn + fn
        precision = tp / (tp + fp)
        recall = tp / (tp + fn)
        f_score = 0 #(2 * precision * recall) / (precision + recall)

        return {"n": n, "precision": precision, "recall": recall, "f_score": f_score}



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

            if (iob["iob"] not in ["I-NP"]) or i == len(iob_tagged) - 1: 
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


    data = chunker.load_training_data(fn)
    training_data = data[:24]
    test_data = data[25:]
    chunker.train(training_data)

    chunker.classifier.show_most_informative_features()
    print(chunker.evaluate(test_data))

#    with open(fn) as f:
#        for l in f:
#            tokens = t.tokenize(l)
#            pos = pos_tags.tag(tokens)
#            for c in chunker.assemble(chunker.predict(pos)):
#                print(c)



