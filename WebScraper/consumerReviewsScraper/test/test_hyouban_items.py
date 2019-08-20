import sqlite3
import unittest
from datetime import datetime
from ..settings import DB_NAME
from ..items.hyouban import HyoubanReview


class TestHyoubanReviewItem(unittest.TestCase):
    test_data = (999, 888, 'test_company', 'test_category', 'test_content', 'test_status',
                 datetime.utcnow(), 777, 'good', datetime.utcnow())

    def setUp(self) -> None:
        self.conn = sqlite3.connect(DB_NAME)

    def test_upsert(self):
        obj = HyoubanReview(review_id=self.test_data[0], company_id=self.test_data[1], company=self.test_data[2],
                            category=self.test_data[3], content=self.test_data[4], status=self.test_data[5],
                            publish_date=self.test_data[6], num_helpful=self.test_data[7], attitude=self.test_data[8],
                            created_datetime=self.test_data[9])
        self._insert(obj)
        existing = self._get_record(obj['review_id'])
        self.assertIsNotNone(existing)
        self.assertEqual(existing[0], obj['review_id'])

        obj['created_datetime'] = datetime.utcnow()
        obj.upsert(self.conn)
        existing = self._get_record(obj['review_id'])
        self.assertIsNotNone(existing)
        self.assertTrue(datetime.strptime(existing[9], '%Y-%m-%d %H:%M:%S.%f') > self.test_data[9])

        self.conn.execute('DELETE FROM hyouban_review WHERE `review_id`=?', (obj['review_id'],))
        self.conn.commit()

    def test_delete(self):
        obj = HyoubanReview(review_id=self.test_data[0], company_id=self.test_data[1], company=self.test_data[2],
                            category=self.test_data[3], content=self.test_data[4], status=self.test_data[5],
                            publish_date=self.test_data[6], num_helpful=self.test_data[7], attitude=self.test_data[8],
                            created_datetime=self.test_data[9])
        self._insert(obj)
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
