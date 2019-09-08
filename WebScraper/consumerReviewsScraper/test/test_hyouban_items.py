import sqlite3
import unittest
from datetime import datetime
from ..settings import DB_NAME
from ..items.hyouban import HyoubanReview


class TestHyoubanReviewItem(unittest.TestCase):
    review_id = 999
    company_id = 888
    company = 'test company'
    category = 'test category'
    content = 'test content'
    status = 'test status'
    publish_date = datetime.utcnow()
    num_helpful = 77
    attitude = 'good'
    created_datetime = datetime.utcnow()

    def setUp(self) -> None:
        self.conn = sqlite3.connect(DB_NAME)

    def test_upsert(self):
        obj = self._get_instance()
        self._insert(obj)
        existing = self._get_record(obj['review_id'])
        self.assertIsNotNone(existing)
        self.assertEqual(existing[0], obj['review_id'])

        obj['created_datetime'] = datetime.utcnow()
        obj.upsert(self.conn)
        existing = self._get_record(obj['review_id'])
        self.assertIsNotNone(existing)
        self.assertTrue(datetime.strptime(existing[9], '%Y-%m-%d %H:%M:%S.%f') > self.created_datetime)

        self.conn.execute('DELETE FROM hyouban_review WHERE `review_id`=?', (obj['review_id'],))
        self.conn.commit()

    def test_delete(self):
        obj = self._get_instance()
        obj.upsert(self.conn)
        existing = self._get_record(obj['review_id'])
        self.assertIsNotNone(existing)
        self.assertEqual(existing[0], obj['review_id'])
        obj.delete(self.conn)
        existing = self._get_record(obj['review_id'])
        self.assertIsNone(existing)

    def tearDown(self) -> None:
        self.conn.close()

    def _insert(self, obj: HyoubanReview) -> None:
        self.conn.execute('INSERT INTO hyouban_review(`review_id`, `company`, `company_id`, `category`, `content`, '
                          '`status`, `publish_date`, `num_helpful`, `attitude`, `created_datetime`) '
                          'VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                          obj.field_values)
        self.conn.commit()
    
    def _get_instance(self) -> HyoubanReview:
        return HyoubanReview(
            review_id=self.review_id,
            company_id=self.company_id,
            company=self.company,
            category=self.category,
            content=self.content,
            status=self.status,
            publish_date=self.publish_date,
            num_helpful=self.num_helpful,
            attitude=self.attitude,
            created_datetime=self.created_datetime)

    def _get_record(self, pk) -> tuple:
        return self.conn.execute('SELECT * FROM hyouban_review WHERE `review_id`=?', (pk,)).fetchone()


def create_test_suite() -> unittest.TestSuite:
    suite = unittest.TestSuite()
    suite.addTests([
        TestHyoubanReviewItem('test_upsert_delete'),
    ])
    return suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(create_test_suite())
