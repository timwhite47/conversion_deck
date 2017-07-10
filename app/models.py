from app import db

class Conversion(db.Model):
    __tablename__ = 'conversions'
    distinct_id = db.Column(db.String, primary_key=True)
    conversion_proba = db.Column(db.Float)
    pricing_page = db.Column('Land on Pricing Page', db.Integer)
    limit_notification = db.Column('Display Limit Notification', db.Integer)
    export_ppt = db.Column('Export PPT', db.Integer)
    new_deck = db.Column('New Deck', db.Integer)
    editor_opened = db.Column('Editor Opened', db.Integer)

    def __init__(self):
        pass

    def __attributes__(self):
        return {
            "distinct_id": self.distinct_id,
            "conversion_proba": self.conversion_proba,
            "pricing_page": self.pricing_page,
            "limit_notification": self.limit_notification,
            "export_ppt": self.export_ppt,
            "new_deck": self.new_deck,
            "editor_opened": self.editor_opened,
        }

class Churn(db.Model):
    __tablename__ = 'churns'

    distinct_id = db.Column(db.String, primary_key=True)
    churn_proba = db.Column(db.Float)

    account_age = db.Column(db.Integer)
    camp_deliveries = db.Column(db.Integer)

    vertical_educator = db.Column(db.Integer)
    vertical_marketing = db.Column(db.Integer)

    slide_start = db.Column('Slide start', db.Integer)
    editor_opened = db.Column('Editor Opened', db.Integer)
    deck_created = db.Column('Deck Created', db.Integer)

    started_onboarding = db.Column('Started Onboarding', db.Integer)
    export = db.Column('Export', db.Integer)
    signin = db.Column('signin', db.Integer)

    def __init__(self):
        pass

    def __attributes__(self):
        return {
            "distinct_id": self.distinct_id,
            "churn_proba": self.churn_proba,
            "account_age": self.account_age,
            "camp_deliveries": self.camp_deliveries,
            "vertical_educator": self.vertical_educator,
            "vertical_marketing": self.vertical_marketing,
            "slide_start": self.slide_start,
            "editor_opened": self.editor_opened,
            "deck_created": self.deck_created,
            "started_onboarding": self.started_onboarding,
            "export": self.export,
            "signin": self.signin,
        }
