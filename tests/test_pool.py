# coding: utf-8
import six
from tornado.ioloop import IOLoop
from tornado import testing
from bson import ObjectId
from mongotor.connection import Connection
from mongotor.pool import ConnectionPool
from mongotor.database import Database
from mongotor.errors import TooManyConnections
from mongotor import message


class ConnectionPoolTestCase(testing.AsyncTestCase):

    def get_new_ioloop(self):
        return IOLoop.instance()

    def test_get_connection(self):
        """[ConnectionPoolTestCase] - Can get a connection"""
        pool = ConnectionPool('localhost', 27027, dbname='test')
        pool.connection(self.stop)
        conn = self.wait()

        self.assertIsInstance(conn, Connection)

    def test_wait_for_connection_when_maxconnection_is_reached(self):
        """[ConnectionPoolTestCase] - Wait for a connection when maxconnections is reached"""

        pool = ConnectionPool('localhost', 27027, dbname='test', maxconnections=1)

        pool.connection(self.stop)
        conn1 = self.wait()

        pool.connection(self.stop)

        conn1.close()
        conn2 = self.wait()

        self.assertIsInstance(conn1, Connection)
        self.assertIsInstance(conn2, Connection)
        self.assertEquals(pool._connections, 1)

    def test_raise_too_many_connection_when_maxconnection_is_reached(self):
        """[ConnectionPoolTestCase] - Raise TooManyConnections connection when maxconnections is reached"""

        pool = ConnectionPool('localhost', 27027, dbname='test', maxconnections=10)

        connections = []
        for i in six.moves.range(10):
            pool.connection(self.stop)
            connections.append(self.wait())

        pool.connection(self.stop)
        self.assertRaises(TooManyConnections, self.wait)

    def test_close_connection_stream_should_be_release_from_pool(self):
        """[ConnectionPoolTestCase] - Release connection from pool when stream is closed"""

        pool = ConnectionPool('localhost', 27027, dbname='test', maxconnections=10)

        pool.connection(self.stop)
        connection = self.wait()

        def release(conn):
            self.assertEquals(conn, connection)
            _release(conn)
            self.stop()

        self.assertEquals(len(pool._idle_connections), 9)

        _release = pool.release
        pool.release = release
        connection._stream.close()

        self.wait()

        self.assertEquals(pool._connections, 0)
        self.assertEquals(len(pool._idle_connections), 10)

    def test_maxusage_in_pool_connections(self):
        """[ConnectionPoolTestCase] - test maxusage in connections"""
        pool = ConnectionPool('localhost', 27027, dbname='test', maxconnections=1, maxusage=299)

        message_test = message.query(0, 'mongotor_test.$cmd', 0, 1,
            {'driverOIDTest': ObjectId()})

        for i in six.moves.range(300):
            pool.connection(self.stop)
            connection = self.wait()

            connection.send_message_with_response(message_test, callback=self.stop)
            self.wait()

        pool.connection(self.stop)
        new_connection = self.wait()

        self.assertEquals(new_connection.usage, 0)
        self.assertNotEqual(new_connection, connection)
        new_connection.send_message_with_response(message_test, callback=self.stop)

        self.wait()

        self.assertEquals(new_connection.usage, 1)

    def test_load_in_pool_connections(self):
        """[ConnectionPoolTestCase] - test load in connections"""
        pool = ConnectionPool('localhost', 27027, dbname='test', maxconnections=10, maxusage=29)

        message_test = message.query(0, 'mongotor_test.$cmd', 0, 1,
            {'driverOIDTest': ObjectId()})

        for i in range(300):
            pool.connection(self.stop)
            connection = self.wait()

            connection.send_message_with_response(message_test, callback=self.stop)
            self.wait()

        self.assertEquals(len(pool._idle_connections), 0)

        for i in six.moves.range(300):
            pool.connection(self.stop)
            connection = self.wait()

            connection.send_message_with_response(message_test, callback=self.stop)
            self.wait()

        self.assertEquals(len(pool._idle_connections), 0)

    def test_load_two_in_pool_connections(self):
        """[ConnectionPoolTestCase] - test load two in connections"""
        pool = ConnectionPool('localhost', 27027, dbname='test', maxconnections=10, maxusage=29)

        message_test = message.query(0, 'mongotor_test.$cmd', 0, 1,
            {'driverOIDTest': ObjectId()})

        for i in six.moves.range(30000):
            pool.connection(self.stop)
            connection = self.wait()

            connection.send_message_with_response(message_test, callback=self.stop)
            self.wait()

        self.assertEquals(len(pool._idle_connections), 0)
        self.assertEquals(pool._connections, 0)

    def test_check_connections_when_use_cursors(self):
        """[ConnectionPoolTestCase] - check connections when use cursors"""
        db = Database.init('localhost:27027', dbname='test', maxconnections=10, maxusage=29)

        try:
            for i in range(2):
                db.cards.insert({'_id': ObjectId(), 'range': i}, callback=self.stop)
                self.wait()

            self.assertEquals(db._nodes[0].pool._connections, 0)

            db.cards.find({}, callback=self.stop)
            self.wait()

            self.assertEquals(db._nodes[0].pool._connections, 0)
        finally:
            Database.disconnect()
