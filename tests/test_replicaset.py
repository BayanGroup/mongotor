# coding: utf-8
from tornado.ioloop import IOLoop
from tornado import testing
from bson import ObjectId
from mongotor.errors import DatabaseError
from mongotor.database import Database
from mongotor.node import ReadPreference
import os
import time


class ReplicaSetTestCase(testing.AsyncTestCase):

    def get_new_ioloop(self):
        return IOLoop.instance()

    def tearDown(self):
        super(ReplicaSetTestCase, self).tearDown()
        Database.disconnect()

    def test_configure_nodes(self):
        """[ReplicaSetTestCase] - Configure nodes"""

        db = Database.init(["localhost:27027", "localhost:27028"], dbname='test')
        db._connect(callback=self.stop)
        self.wait()

        master_node = ReadPreference.select_primary_node(Database()._nodes)
        secondary_node = ReadPreference.select_node(Database()._nodes, mode=ReadPreference.SECONDARY)

        self.assertEquals(master_node.host, 'localhost')
        self.assertEquals(master_node.port, 27027)

        self.assertEquals(secondary_node.host, 'localhost')
        self.assertEquals(secondary_node.port, 27028)

        nodes = Database()._nodes
        self.assertEquals(len(nodes), 2)

    def test_raises_error_when_mode_is_secondary_and_secondary_is_down(self):
        """[ReplicaSetTestCase] - Raise error when mode is secondary and secondary is down"""
        os.system('make mongo-kill-node2')
        time.sleep(1)  # stops are fast

        try:
            db = Database.init(["localhost:27027", "localhost:27028"], dbname='test')
            db._connect(callback=self.stop)
            self.wait()

            self.assertRaises(DatabaseError, Database().send_message, (None, ''),
                              read_preference=ReadPreference.SECONDARY)
        finally:
            os.system('make mongo-start-node2')
            time.sleep(8)  # wait to become secondary again


class SecondaryPreferredTestCase(testing.AsyncTestCase):

    def get_new_ioloop(self):
        return IOLoop.instance()

    def tearDown(self):
        super(SecondaryPreferredTestCase, self).tearDown()
        Database._instance = None

    def test_find_on_secondary(self):
        """[SecondaryPreferredTestCase] - test find document from secondary"""
        db = Database.init(["localhost:27027", "localhost:27028"], dbname='test',
            read_preference=ReadPreference.SECONDARY_PREFERRED)
        db._connect(callback=self.stop)
        self.wait()

        doc = {'_id': ObjectId()}
        db.test.insert(doc, callback=self.stop)
        self.wait()

        time.sleep(2)
        db.test.find_one(doc, callback=self.stop)
        doc_found, error = self.wait()

        self.assertEquals(doc_found, doc)
