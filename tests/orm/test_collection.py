# coding: utf-8
import six
from tornado.ioloop import IOLoop
from tornado import testing
from mongotor.database import Database
from mongotor.orm.collection import Collection
from mongotor.orm.manager import Manager
from mongotor.orm.field import (ObjectIdField, StringField, DateTimeField,
    IntegerField, BooleanField, FloatField, ListField, ObjectField,
    DecimalField, UrlField, UuidField, EmailField, Md5Field, Sha1Field)
from mongotor.errors import DatabaseError
from bson.objectid import ObjectId
import sure
import uuid
import hashlib


class CollectionTestCase(testing.AsyncTestCase):

    def get_new_ioloop(self):
        return IOLoop.instance()

    def setUp(self):
        super(CollectionTestCase, self).setUp()
        Database.init(["localhost:27027", "localhost:27028"], dbname='test')

    def tearDown(self):
        super(CollectionTestCase, self).tearDown()
        Database.disconnect()

    def test_save_a_new_document(self):
        """[CollectionTestCase] - Save a new document using collection schema"""
        class CollectionTest(Collection):
            __collection__ = "collection_test"
            _id = ObjectIdField()
            string_attr = StringField()

        doc_test = CollectionTest()
        doc_test._id = ObjectId()
        doc_test.string_attr = "should be string value"

        doc_test.save(callback=self.stop)
        response, error = self.wait()

        response['ok'].should.be.equal(1.0)
        error.should.be.none

    def test_remove_a_document(self):
        """[CollectionTestCase] - Remove a document"""
        class CollectionTest(Collection):
            __collection__ = "collection_test"
            _id = ObjectIdField()
            string_attr = StringField()

        doc_test = CollectionTest()
        doc_test._id = ObjectId()
        doc_test.string_attr = "should be string value"

        doc_test.save(callback=self.stop)
        self.wait()

        doc_test.remove(callback=self.stop)
        response, error = self.wait()

        response['ok'].should.be.equal(1.0)
        error.should.be.none

    def test_update_a_document(self):
        """[CollectionTestCase] - Update a document"""
        class CollectionTest(Collection):
            __collection__ = "collection_test"
            _id = ObjectIdField()
            string_attr = StringField()

        doc_test = CollectionTest()
        doc_test._id = ObjectId()
        doc_test.string_attr = "should be string value"

        doc_test.save(callback=self.stop)
        self.wait()

        doc_test.string_attr = "should be new string value"
        doc_test.update(callback=self.stop)
        response, error = self.wait()

        response['ok'].should.be.equal(1.0)
        error.should.be.none

    def test_can_create_collection_from_dictionary(self):
        """[CollectionTestCase] - Create a document from dictionary """
        object_id = ObjectId()
        object_dict = {
            'string_attr': 'string_attr',
            'integer_attr': 1,
            'bool_attr': True,
            'float_attr': 1.0,
            'list_attr': [1, 2, 3],
            'object_attr': {'chave': 'valor'},
            'object_id_attr': object_id,
        }

        class CollectionTest(Collection):
            string_attr = StringField()
            integer_attr = IntegerField()
            bool_attr = BooleanField()
            float_attr = FloatField()
            list_attr = ListField()
            object_attr = ObjectField()
            object_id_attr = ObjectIdField()
            unknow_object = StringField()

        object_instance = CollectionTest.create(object_dict)

        object_instance.string_attr.should.be.equal('string_attr')
        object_instance.integer_attr.should.be.equal(1)
        object_instance.bool_attr.should.be.ok
        object_instance.float_attr.should.be.equal(1.0)
        object_instance.list_attr.should.be.equal([1, 2, 3])
        object_instance.object_attr.should.be.equal({'chave': 'valor'})
        object_instance.object_id_attr.should.be.equal(object_id)

    def test_create_attribute_if_model_does_not_contains_field(self):
        """[CollectionTestCase] - Create attribute if model does not contains field"""
        class CollectionTest(Collection):
            string_attr = StringField()

        object_dict = {
            'string_attr': 'string_attr',
            'integer_attr': 1
        }

        object_instance = CollectionTest.create(object_dict)
        object_instance.string_attr.should.be.equal('string_attr')
        object_instance.integer_attr.should.be.equal(1)

    def test_ignore_attribute_with_different_field_type(self):
        """[CollectionTestCase] - Ignore attributes with different field type"""
        class CollectionTest(Collection):
            string_attr = DateTimeField()

        object_dict = {
            'string_attr': 'duvido'
        }

        object_instance = CollectionTest.create(object_dict)
        object_instance.string_attr.should.be.none

    def test_can_set_manager_object_in_collection(self):
        """[CollectionTestCase] - Can set manager object in collection"""
        class CollectionTest(Collection):
            should_be_value = StringField()

        CollectionTest.objects.should.be.a('mongotor.orm.manager.Manager')

    def test_can_be_load_lazy_class(self):
        """[CollectionTestCase] - Can be load lazy collection"""
        class CollectionTest(Collection):
            pass

        issubclass(Collection("CollectionTest"), CollectionTest).should.be.ok

    def test_can_be_load_child_lazy_class(self):
        """[CollectionTestCase] - Can be load lazy child collection"""
        class CollectionTest(Collection):
            pass

        class ChildCollectionTest(CollectionTest):
            pass

        issubclass(Collection("ChildCollectionTest"),\
            ChildCollectionTest).should.be.ok

    def test_raises_erro_when_use_collection_with_not_initialized_database(self):
        """[CollectionTestCase] - Raises DatabaseError when use collection with a not initialized database"""

        class CollectionTest(Collection):
            __collection__ = 'collection_test'

        Database.disconnect()
        CollectionTest().save(callback=None)
        self.assertRaises(DatabaseError, self.wait)

        Database.init(["localhost:27027", "localhost:27028"], dbname='test')

    def test_update_tracks_changed_attrs(self):
        """[CollectionTestCase] - Update a document and track dirty fields"""
        class CollectionTest(Collection):
            __collection__ = "collection_test"
            _id = ObjectIdField()
            string_attr = StringField()

        doc_test = CollectionTest()
        doc_test._id = ObjectId()
        doc_test.string_attr = "should be string value"

        doc_test.save(callback=self.stop)
        self.wait()

        doc_test.string_attr = "should be new string value"
        "string_attr".should.be.within(doc_test.dirty_fields)
        "_id".shouldnot.be.within(doc_test.dirty_fields)

    def test_update_tracks_changed_object_attrs(self):
        """[CollectionTestCase] - Update a document and track dirty object fields"""
        class CollectionTest(Collection):
            __collection__ = "collection_test"
            _id = ObjectIdField()
            string_attr = StringField()
            object_field = ObjectField()

        doc_test = CollectionTest()
        doc_test._id = ObjectId()
        doc_test.string_attr = "should be string value"

        doc_test.save(callback=self.stop)
        self.wait()

        doc_test.object_field = {'name': 'should be a new'}
        "object_field".should.be.within(doc_test.dirty_fields)
        "_id".shouldnot.be.within(doc_test.dirty_fields)

    def test_load_obj_does_not_set_dirty_keys(self):
        """[CollectionTestCase] - Check if freshly loaded document has no dirty fields"""
        class CollectionTest(Collection):
            __collection__ = "collection_test"
            _id = ObjectIdField()
            string_attr = StringField()

        doc_test = CollectionTest()
        doc_test._id = ObjectId()
        doc_test.string_attr = "should be string value"

        doc_test.save(callback=self.stop)
        self.wait()
        CollectionTest.objects.find_one(query=doc_test._id, callback=self.stop)
        db_doc_test = self.wait()
        db_doc_test.dirty_fields.should.be.empty

    def test_force_update(self):
        class CollectionTest(Collection):
            __collection__ = "collection_test"
            _id = ObjectIdField()
            string_attr = StringField()

        doc_test = CollectionTest()
        doc_test._id = ObjectId()
        doc_test.string_attr = "should be string value"

        doc_test.save(callback=self.stop)
        self.wait()

        doc_test.string_attr = "changed"

        doc_test.update(callback=self.stop, force=True)
        self.wait()

        CollectionTest.objects.find_one(query=doc_test._id, callback=self.stop)
        db_doc_test = self.wait()

        db_doc_test.string_attr.should.be.equal("changed")

    def test_empty_callback(self):
        class CollectionTest(Collection):
            __collection__ = "collection_test"
            _id = ObjectIdField()
            string_attr = StringField()

        doc_test = CollectionTest()
        doc_test._id = ObjectId()
        doc_test.string_attr = "should be string value"

        doc_test.save(callback=self.stop)
        db_doc_test = self.wait()

        doc_test.update(callback=self.stop)
        db_doc_test = self.wait()

        db_doc_test.should.be(tuple())

    def test_get_fields_from_base_classes(self):
        class CollectionTest(Collection):
            __collection__ = "collection_test"
            _id = ObjectIdField(default=ObjectId())
            base_url_field = UrlField(default="https://www.test.com")
            base_decimal_field = DecimalField(default=2.1)
            base_md5_field = Md5Field(default=hashlib.md5(b"test").hexdigest())

        class ChildCollectionTest(CollectionTest):
            child_uuid_field = UuidField(default=uuid.uuid4())
            child_email_field = EmailField(default="test@test.com")
            child_sha1_field = Sha1Field(default=hashlib.sha1(b"test").hexdigest())

        if six.PY3:  # only test LongField on python2
            return

        from mongotor.orm.field import LongField
        class SecondChildCollectionTest(ChildCollectionTest):
            second_child_long_field = LongField(default=1000)

        test_instance = SecondChildCollectionTest()
        test_dict = test_instance.as_dict()
        test_dict.should.be.length_of(8)
