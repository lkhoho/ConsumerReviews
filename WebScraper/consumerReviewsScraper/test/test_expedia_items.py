import simplejson as json
import sqlite3
import unittest
from datetime import datetime
from ..settings import DB_NAME
from ..items.expedia import ExpediaHotelItem, ExpediaReviewItem


class TestExpediaHotelItem(unittest.TestCase):
    hotel_id = 1234567
    name = 'test_hotel'
    nearby_info = ['Tokyo Midtown - 7 min drive', 'Toyosu Market - 8 min drive', 'LaLaport Toyosu Mall - 9 min drive',
                   'Tokyo Tower - 14 min drive', 'Mori Art Museum - 16 min drive']
    around_info = ['Tennozu Isle Station - 2 min walk', 'Tokyo Shimbamba Station - 16 min walk',
                   'Tokyo (HND-Haneda) - 9 min drive']
    introduction = '''Located on the waterfront, Daiichi Hotel Tokyo Seafort is in Tokyo's Shinagawa neighborhood,
                    a walkable area with good shopping. Tokyo Tower and Sengakuji Temple are notable landmarks,
                    and some of the area's activities can be experienced at LEGOLAND Discovery Center Tokyo and
                    Oedo Onsen Monogatari Hot Springs. Looking to enjoy an event or a game? See what's going on at
                    Nippon Budokan or Tokyo Dome. Our guests appreciate the hotel's central location.'''
    created_datetime = datetime.utcnow()

    def setUp(self) -> None:
        self.conn = sqlite3.connect(DB_NAME)

    def test_upsert(self):
        obj = self._get_instance()
        obj.upsert(self.conn)
        existing = self._get_record(obj['hotel_id'])
        self.assertIsNotNone(existing)
        self.assertEqual(existing[1], obj['hotel_id'])
        obj['around_info'] = json.dumps([])
        obj.upsert(self.conn)
        existing = self._get_record(obj['hotel_id'])
        self.assertIsNotNone(existing)
        self.assertEqual(json.loads(existing[4]), json.loads(obj['around_info']))
        self.conn.execute('DELETE FROM `expedia_hotel` WHERE `hotel_id`=?', (obj['hotel_id'],))
        self.conn.commit()

    def test_delete(self):
        obj = self._get_instance()
        obj.upsert(self.conn)
        existing = self._get_record(obj['hotel_id'])
        self.assertIsNotNone(existing)
        self.assertEqual(existing[1], obj['hotel_id'])
        obj.delete(self.conn)
        existing = self._get_record(obj['hotel_id'])
        self.assertIsNone(existing)

    def tearDown(self) -> None:
        self.conn.close()

    def _get_instance(self) -> ExpediaHotelItem:
        return ExpediaHotelItem(
            hotel_id=self.hotel_id,
            name=self.name,
            nearby_info=json.dumps(self.nearby_info),
            around_info=json.dumps(self.around_info),
            introduction=self.introduction,
            created_datetime=self.created_datetime
        )

    def _get_record(self, pk) -> tuple:
        return self.conn.execute('SELECT * FROM `expedia_hotel` WHERE `hotel_id`=?', (pk,)).fetchone()


class TestExpediaReviewItem(unittest.TestCase):
    review_id = '1eb9c8a8-bd15-431c-9471-e54e4a4a63cc'
    author = 'test author'
    publish_datetime = datetime.utcnow()
    content = 'test content'
    created_datetime = datetime.utcnow()
    overall_rating = 3
    num_helpful = 12
    stay_duration = 'stayed 3 nights on Aug 2018'
    response_id = '2badce45-856a-4f5f-a465-0a0edce71248'
    response_author = 'test response author'
    response_publish_datetime = datetime.utcnow()
    response_content = 'test response content'
    response_display_locale = 'enAU'
    superlative = 'Okay'
    title = 'not exist'
    locale = 'enUS'
    location = 'Redmond, WA'
    remarks_positive = 'N/A'
    remarks_negative = 'N/A'
    remarks_location = 'N/A'
    hotel_id = 1234567

    def setUp(self) -> None:
        self.conn = sqlite3.connect(DB_NAME)

    def test_upsert(self):
        obj = self._get_instance()
        obj.upsert(self.conn)
        existing = self._get_record(obj['review_id'])
        self.assertIsNotNone(existing)
        self.assertEqual(existing[1], obj['review_id'])
        obj['superlative'] = '很好'
        obj['locale'] = 'zhCN'
        obj.upsert(self.conn)
        existing = self._get_record(obj['review_id'])
        self.assertEqual('很好', existing[13])
        self.assertEqual('zhCN', existing[15])
        self.conn.execute('DELETE FROM `expedia_review` WHERE `review_id`=?', (obj['review_id'],))
        self.conn.commit()

    def test_delete(self):
        obj = self._get_instance()
        obj.upsert(self.conn)
        existing = self._get_record(obj['review_id'])
        self.assertIsNotNone(existing)
        self.assertEqual(existing[1], obj['review_id'])
        obj.delete(self.conn)
        existing = self._get_record(obj['review_id'])
        self.assertIsNone(existing)

    def tearDown(self) -> None:
        self.conn.close()

    def _get_instance(self) -> ExpediaReviewItem:
        return ExpediaReviewItem(
            review_id=self.review_id,
            author=self.author,
            publish_datetime=self.publish_datetime,
            content=self.content,
            created_datetime=self.created_datetime,
            overall_rating=self.overall_rating,
            num_helpful=self.num_helpful,
            stay_duration=self.stay_duration,
            response_id=self.response_id,
            response_author=self.response_author,
            response_publish_datetime=self.response_publish_datetime,
            response_content=self.response_content,
            response_display_locale=self.response_display_locale,
            superlative=self.superlative,
            title=self.title,
            locale=self.locale,
            location=self.location,
            remarks_positive=self.remarks_positive,
            remarks_negative=self.remarks_negative,
            remarks_location=self.remarks_location,
            hotel_id=self.hotel_id
        )

    def _get_record(self, pk) -> tuple:
        return self.conn.execute('SELECT * FROM `expedia_review` WHERE `review_id`=?', (pk,)).fetchone()


def create_test_suite() -> unittest.TestSuite:
    suite = unittest.TestSuite()
    suite.addTests([
        TestExpediaHotelItem('test_upsert_delete'),
        TestExpediaReviewItem('test_upsert_delete'),
    ])
    return suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(create_test_suite())
