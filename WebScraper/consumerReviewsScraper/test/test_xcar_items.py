import sqlite3
import unittest
from datetime import datetime, timedelta
from ..settings import DB_NAME
from ..items.xcar import XcarUser, XcarPost, XcarThread, XcarForum


class TestXcarForumItem(unittest.TestCase):
    test_data = (999, 'test_forum', datetime.utcnow())

    def setUp(self) -> None:
        self.conn = sqlite3.connect(DB_NAME)

    def test_upsert(self):
        obj = XcarForum(fid=self.test_data[0], name=self.test_data[1], created_datetime=self.test_data[2])
        self._insert(obj)
        existing = self._get_record(obj['fid'])
        self.assertIsNotNone(existing)
        self.assertEqual(existing[0], obj['fid'])

        obj['created_datetime'] = datetime.utcnow()
        obj.upsert(self.conn)
        existing = self._get_record(obj['fid'])
        self.assertIsNotNone(existing)
        self.assertTrue(datetime.strptime(existing[2], '%Y-%m-%d %H:%M:%S.%f') > self.test_data[2])

        self.conn.execute('DELETE FROM xcar_forum WHERE `fid`=?', (obj['fid'],))
        self.conn.commit()

    def test_delete(self):
        obj = XcarForum(fid=self.test_data[0], name=self.test_data[1], created_datetime=self.test_data[2])
        self._insert(obj)
        existing = self._get_record(obj['fid'])
        self.assertIsNotNone(existing)
        self.assertEqual(existing[0], obj['fid'])
        obj.delete(self.conn)
        existing = self._get_record(obj['fid'])
        self.assertIsNone(existing)

    def tearDown(self) -> None:
        self.conn.close()

    def _insert(self, obj: XcarForum) -> None:
        self.conn.execute('INSERT INTO xcar_forum(`fid`, `name`, `created_datetime`) VALUES(?, ?, ?)',
                          obj.field_values)
        self.conn.commit()

    def _get_record(self, pk) -> tuple:
        return self.conn.execute('SELECT * FROM xcar_forum WHERE `fid`=?', (pk,)).fetchone()


class TestXcarPostItem(unittest.TestCase):
    test_data = (999, 'user1', 'content for test', datetime.utcnow() - timedelta(days=30), datetime.utcnow(), 1, 888)

    def setUp(self) -> None:
        self.conn = sqlite3.connect(DB_NAME)

    def test_upsert(self):
        obj = XcarPost(pid=self.test_data[0], author=self.test_data[1], content=self.test_data[2],
                       publish_datetime=self.test_data[3], created_datetime=self.test_data[4],
                       is_flag=self.test_data[5], thread=self.test_data[6])
        self._insert(obj)
        existing = self._get_record(obj['pid'])
        self.assertIsNotNone(existing)
        self.assertEqual(existing[0], obj['pid'])

        new_content = 'another content for test'
        obj['content'] = new_content
        obj['publish_datetime'] = datetime.utcnow()
        obj.upsert(self.conn)
        existing = self._get_record(obj['pid'])
        self.assertIsNotNone(existing)
        self.assertEqual(obj['content'], new_content)
        self.assertTrue(datetime.strptime(existing[3], '%Y-%m-%d %H:%M:%S.%f') > self.test_data[3])

        self.conn.execute('DELETE FROM xcar_post WHERE `pid`=?', (obj['pid'],))
        self.conn.commit()

    def test_delete(self):
        obj = XcarPost(pid=self.test_data[0], author=self.test_data[1], content=self.test_data[2],
                       publish_datetime=self.test_data[3], created_datetime=self.test_data[4],
                       is_flag=self.test_data[5], thread=self.test_data[6])
        self._insert(obj)
        existing = self._get_record(obj['pid'])
        self.assertIsNotNone(existing)
        self.assertEqual(existing[0], obj['pid'])
        obj.delete(self.conn)
        existing = self._get_record(obj['pid'])
        self.assertIsNone(existing)

    def tearDown(self) -> None:
        self.conn.close()

    def _insert(self, obj: XcarPost) -> None:
        self.conn.execute('INSERT INTO xcar_post(`pid`, `author`, `content`, `publish_datetime`, `created_datetime`, '
                          '`is_flag`, `thread`) VALUES(?, ?, ?, ?, ?, ?, ?)', obj.field_values)
        self.conn.commit()

    def _get_record(self, pk) -> tuple:
        return self.conn.execute('SELECT * FROM xcar_post WHERE `pid`=?', (pk,)).fetchone()


class TestXcarThreadItem(unittest.TestCase):
    test_data = (999, 'user1', 'content for test', datetime.utcnow() - timedelta(days=30), datetime.utcnow(), 1, 888)

    def setUp(self) -> None:
        self.conn = sqlite3.connect(DB_NAME)

    def test_upsert(self):
        obj = XcarPost(pid=self.test_data[0], author=self.test_data[1], content=self.test_data[2],
                       publish_datetime=self.test_data[3], created_datetime=self.test_data[4],
                       is_flag=self.test_data[5], thread=self.test_data[6])
        self._insert(obj)
        existing = self._get_record(obj['pid'])
        self.assertIsNotNone(existing)
        self.assertEqual(existing[0], obj['pid'])

        new_content = 'another content for test'
        obj['content'] = new_content
        obj['publish_datetime'] = datetime.utcnow()
        obj.upsert(self.conn)
        existing = self._get_record(obj['pid'])
        self.assertIsNotNone(existing)
        self.assertEqual(obj['content'], new_content)
        self.assertTrue(datetime.strptime(existing[3], '%Y-%m-%d %H:%M:%S.%f') > self.test_data[3])

        self.conn.execute('DELETE FROM xcar_post WHERE `pid`=?', (obj['pid'],))
        self.conn.commit()

    def test_delete(self):
        obj = XcarPost(pid=self.test_data[0], author=self.test_data[1], content=self.test_data[2],
                       publish_datetime=self.test_data[3], created_datetime=self.test_data[4],
                       is_flag=self.test_data[5], thread=self.test_data[6])
        self._insert(obj)
        existing = self._get_record(obj['pid'])
        self.assertIsNotNone(existing)
        self.assertEqual(existing[0], obj['pid'])
        obj.delete(self.conn)
        existing = self._get_record(obj['pid'])
        self.assertIsNone(existing)

    def tearDown(self) -> None:
        self.conn.close()

    def _insert(self, obj: XcarPost) -> None:
        self.conn.execute('INSERT INTO xcar_post(`pid`, `author`, `content`, `publish_datetime`, `created_datetime`, '
                          '`is_flag`, `thread`) VALUES(?, ?, ?, ?, ?, ?, ?)', obj.field_values)
        self.conn.commit()

    def _get_record(self, pk) -> tuple:
        return self.conn.execute('SELECT * FROM xcar_post WHERE `pid`=?', (pk,)).fetchone()
