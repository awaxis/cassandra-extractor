import unittest
import logging
from wadlabs.hub.migration.worker import Worker

log = logging.getLogger('TestWorker')
logging.basicConfig(level=logging.INFO)


class TestWorker(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        log.info('Init Worker tests')

    def testWorkerConvertColumns(self):
        pass

if __name__ == '__main__':
    unittest.main()
