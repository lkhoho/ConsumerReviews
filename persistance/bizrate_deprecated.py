
from persistance import MySqlSession
from domain.exposed.entities import BizrateReview, BizrateSiteExperienceRatings, BizrateAfterPurchaseRatings


class MySqlPersistanceError(Exception):
    def __init__(self, message, review, siteExperience, afterPurchase):
        super().__init__(message)
        self.review = review
        self.siteExp = siteExperience
        self.after = afterPurchase


def get_connection(engine):
    return engine.connect()


def disconnect(conn):
    conn.disconnect()


def get_current_session():
    return MySqlSession()


def insert_bizrate_review(review, site_experience_ratings, after_purchase_ratings):
    try:
        session = get_current_session()
        review.site_experience_ratings = site_experience_ratings
        review.after_purchase_ratings = after_purchase_ratings
        session.add(review)
        session.commit()
    except Exception as err:
        raise MySqlPersistanceError('Failed to insert bizrate review.', review, site_experience_ratings,
                                    after_purchase_ratings)


def insert_bizrate_reviews(reviews: list, site_experience_ratings: list, after_purchase_ratings: list):
    if len(reviews) != len(site_experience_ratings):
        raise MySqlPersistanceError('')
