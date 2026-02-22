
class FakeSectionHead(object):
    def __init__(self, fp):
        self.fp = open(fp)
        self.lines = ['[DEFAULT]'] + self.fp.readlines()

    def __iter__(self):
        return iter(self.lines)
