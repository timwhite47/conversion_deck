from app import db

class Conversion(db.Model):
    __tablename__ = 'conversions'
    distinct_id = db.Column(db.String, primary_key=True)
    conversion_proba = db.Column(db.Float)

    def __init__(self):
        pass

    def __attributes__(self):
        return {
            "distinct_id": self.distinct_id,
            "conversion_proba": self.conversion_proba
        }

class Churn(db.Model):
    __tablename__ = 'churns'
    distinct_id = db.Column(db.String, primary_key=True)
    churn_proba = db.Column(db.Float)

    def __init__(self):
        pass

    def __attributes__(self):
        return {
            "distinct_id": self.distinct_id,
            "churn_proba": self.churn_proba
        }
