import nltk

class PosTags:

    def tag(self, t):
        '''
        With a list of tokens, mark their part of speech and return
        a list dicts (no native tuple type in dataframes it seems).
        '''
        pos = nltk.pos_tag(t)
        retval = []
        for p in pos:
            retval.append({"word": p[0], "tag": p[1]})
        return retval


if __name__ == "__main__":
    pos_tags = PosTags()
    pos = pos_tags.tag(["Hello", "my", "name", "is", "Mace", "Windoo"])
    print(pos)
