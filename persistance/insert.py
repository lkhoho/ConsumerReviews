import re
import simplejson as json
from pymongo import MongoClient
import pymysql as sql

client = MongoClient()
db = client.get_database('ConsumerReviews')
collection = db.get_collection('bizrate')
docs = list(collection.find())
client.close()


def norm_json(element):
    s = json.dumps(element).replace('\'', '')
    return re.sub(u'(\u2018|\u2019)', '', s)


def norm_str(d):
    s = d if len(d) > 0 else None
    return re.sub(u'(\u2018|\u2019)', '', s) if s is not None else None


def norm_int(value):
    return value if value != -1 else None


insert_review = "INSERT INTO `BIZRATE_REVIEW`(`CREATE_TIME`, `SOURCE`, `STORE`, `SHOP_AGAIN`, " \
                "`TO_RECOMMEND`, `SATISFACTION`, `BEFORE_AUTHOR`, `BEFORE_DATE`, " \
                "`BEFORE_REVIEW`, `AFTER_AUTHOR`, `AFTER_DATE`, `AFTER_REVIEW`) VALUES (" \
                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

insert_ratings_site_experience = "INSERT INTO `BIZRATE_SITE_EXPERIENCE_RATINGS`(`CREATE_TIME`, `FK_REVIEW_PID`, " \
                                 "`SHIPPING_OPTIONS`, `SHIPPING_CHARGES`, `EASE_OF_FINDING`, " \
                                 "`PRICE_RELATIVE_TO_OTHER`, `CLARITY_PRODUCT_INFO`, `CHARGES_STATED_CLEARLY`, " \
                                 "`SITE_DESIGN`, `PRODUCT_SELECTION`,`CHECKOUT_SATISFACTION`) VALUES (" \
                                 "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

insert_ratings_after_purchase = "INSERT INTO `BIZRATE_AFTER_PURCHASE_RATINGS`(`CREATE_TIME`, " \
                                "`FK_REVIEW_PID`, `PRODUCT_AVAILABILITY`, `ONTIME_DELIVERY`, " \
                                "`PRODUCT_MET_EXPECTATIONS`, `ORDER_TRACKING`, `RETURN_PROCESS`, " \
                                "`CUSTOMER_SUPPORT`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

conn = sql.connect(host='localhost', port=3306, user='root', password='welcome', database='CONSUMER_REVIEWS',
                   charset='utf8mb4')
cursor = conn.cursor()
s1, s2, s3 = '', '', ''

try:
    for doc in docs:
        s1 = insert_review
        #         print(s1)
        cursor.execute(s1, (norm_str(doc['scrapedDate']), norm_str(doc['source']), norm_str(doc['reviewStore']),
                            norm_int(doc['wouldShopHereAgain']), norm_int(doc['likelihoodToRecommend']),
                            norm_int(doc['overallSatisfaction']), norm_str(doc['author']), norm_str(doc['date']),
                            norm_str(doc['content']), norm_str(doc['reviewAfterPurchase']['author']),
                            norm_str(doc['reviewAfterPurchase']['date']),
                            norm_str(doc['reviewAfterPurchase']['content'])))
        pid = cursor.lastrowid

        s2 = insert_ratings_site_experience
        #         print(s2)
        cursor.execute(s2, (norm_str(doc['scrapedDate']), pid,
                            norm_int(doc['ratingsSiteExperience']['varietyShippingOptions']),
                            norm_int(doc['ratingsSiteExperience']['shippingCharges']),
                            norm_int(doc['ratingsSiteExperience']['easeOfFinding']),
                            norm_int(doc['ratingsSiteExperience']['priceRelativeOtherRetailers']),
                            norm_int(doc['ratingsSiteExperience']['clarityProductInfo']),
                            norm_int(doc['ratingsSiteExperience']['chargesStatedClearly']),
                            norm_int(doc['ratingsSiteExperience']['designSite']),
                            norm_int(doc['ratingsSiteExperience']['productSelection']),
                            norm_int(doc['ratingsSiteExperience']['satisfactionCheckout'])))

        s3 = insert_ratings_after_purchase
        #         print(s3)
        cursor.execute(s3, (norm_str(doc['scrapedDate']), pid,
                            norm_int(doc['ratingsAfterPurchase']['productAvailability']),
                            norm_int(doc['ratingsAfterPurchase']['onTimeDelivery']),
                            norm_int(doc['ratingsAfterPurchase']['productMetExpectations']),
                            norm_int(doc['ratingsAfterPurchase']['orderTracking']),
                            norm_int(doc['ratingsAfterPurchase']['returnsProcess']),
                            norm_int(doc['ratingsAfterPurchase']['customerSupport'])))
        conn.commit()

        cursor.execute('SELECT COUNT(*) AS c1 FROM `BIZRATE_REVIEW`')
        c1 = cursor.fetchone()
        cursor.execute('SELECT COUNT(*) AS c2 FROM `BIZRATE_SITE_EXPERIENCE_RATINGS`')
        c2 = cursor.fetchone()
        cursor.execute('SELECT COUNT(*) AS c3 FROM `BIZRATE_AFTER_PURCHASE_RATINGS`')
        c3 = cursor.fetchone()
        assert c1[0] == c2[0] == c3[0]
except Exception as exc:
    print('Exception = ' + str(exc))
    print('s1 = ' + s1)
    print('s2 = ' + s2)
    print('s3 = ' + s3)
    conn.rollback()
finally:
    conn.close()
