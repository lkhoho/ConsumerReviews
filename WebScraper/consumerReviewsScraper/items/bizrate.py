import scrapy


class BizrateStoreItem(scrapy.Item):
    """
    Stores on bizrate.com.
    """

    store_id = scrapy.Field()
    name = scrapy.Field()
    description = scrapy.Field()
    website = scrapy.Field()
    is_certified = scrapy.Field()
    award_year = scrapy.Field()
    award_tier = scrapy.Field()
    award_won_years = scrapy.Field()
    created_datetime = scrapy.Field()
    
    # overall ratings
    overall_satisfaction = scrapy.Field()
    shop_again = scrapy.Field()
    to_recommend = scrapy.Field()
    
    # site and shopping experience ratings
    exp_ease_to_find = scrapy.Field()
    exp_site_design = scrapy.Field()
    exp_satisfaction_checkout = scrapy.Field()
    exp_product_selection = scrapy.Field()
    exp_clarity_product_info = scrapy.Field()
    exp_charges_clearly = scrapy.Field()
    exp_price_relative_other = scrapy.Field()
    exp_shipping_charges = scrapy.Field()
    exp_variety_shipping = scrapy.Field()
    
    # after purchase ratings
    after_deliver_ontime = scrapy.Field()
    after_order_tracking = scrapy.Field()
    after_product_met_expectations = scrapy.Field()
    after_customer_support = scrapy.Field()
    after_product_availability = scrapy.Field()
    after_returns_process = scrapy.Field()


class BizrateReviewItem(scrapy.Item):
    """
    Review on bizrate.com.
    """

    review_id = scrapy.Field()
    store_id = scrapy.Field()
    author = scrapy.Field()
    before_purchase_publish_date = scrapy.Field()
    before_purchase_content = scrapy.Field()
    after_purchase_publish_date = scrapy.Field()
    after_purchase_content = scrapy.Field()
    overall_satisfaction = scrapy.Field()
    shop_again = scrapy.Field()
    to_recommend = scrapy.Field()
    created_datetime = scrapy.Field()
    
    # site and shopping experience ratings
    exp_ease_to_find = scrapy.Field()
    exp_site_design = scrapy.Field()
    exp_satisfaction_checkout = scrapy.Field()
    exp_product_selection = scrapy.Field()
    exp_clarity_product_info = scrapy.Field()
    exp_charges_clearly = scrapy.Field()
    exp_price_relative_other = scrapy.Field()
    exp_shipping_charges = scrapy.Field()
    exp_variety_shipping = scrapy.Field()
    
    # after purchase ratings
    after_deliver_ontime = scrapy.Field()
    after_order_tracking = scrapy.Field()
    after_product_met_expectations = scrapy.Field()
    after_customer_support = scrapy.Field()
    after_product_availability = scrapy.Field()
    after_returns_process = scrapy.Field()
