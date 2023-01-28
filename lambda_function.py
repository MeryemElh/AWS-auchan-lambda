
from sqlalchemy import create_engine, Column, ForeignKey, Integer, String, DateTime, Boolean, Float
from sqlalchemy.orm import declarative_base, relationship, Session
from datetime import datetime
import logging
import boto3
import json
import urllib.parse

s3 = boto3.client('s3')

#rds settings
pg_host = "postgres.ckxrkh97gyop.eu-west-3.rds.amazonaws.com"
pg_port = 5432
pg_user = "postgres"
pg_pwd = "postgres"
pg_db = "postgres"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    Base = declarative_base()

    class Product(Base):
        __tablename__ = "product"
        url = Column(String, primary_key=True, nullable=False)
        data = relationship(
            "ProductData",
            back_populates="product",
            cascade="all, delete",
            passive_deletes=True,
        )

    class ProductData(Base):
        __tablename__ = "product_data"
        id = Column(Integer, primary_key=True)
        name = Column(String, nullable=False)
        availability = Column(Boolean)
        s3_path = Column(String)
        timestamp = Column(DateTime)
        rating_count = Column(String) # Can be '> 200'
        rating_value = Column(Float)
        brand = Column(String)
        price_currency = Column(String)
        price_unit = Column(Float)
        price_base_value = Column(Float)
        price_base_unit = Column(String)
        shop = Column(String)
        icon_alt = Column(String)
        icon_src = Column(String)
        icon_s3_path = Column(String)

        url = Column(String, ForeignKey("product.url", ondelete="CASCADE"), nullable=False)
        product = relationship("Product", back_populates="data")
        
        additional_attributes = relationship(
            "Attribute",
            back_populates="product_data",
            cascade="all, delete",
            passive_deletes=True,
        )
        
        variants = relationship(
            "Variant",
            back_populates="product_data",
            cascade="all, delete",
            passive_deletes=True,
        )

        category_id = Column(Integer, ForeignKey("category.id"), nullable=False)
        category = relationship("Category", back_populates="product_data")

    class Attribute(Base):
        __tablename__ = "attribute"
        id = Column(Integer, primary_key=True)
        type = Column(String(50))

        product_data_id = Column(Integer, ForeignKey("product_data.id", ondelete="CASCADE"), nullable=False)
        product_data = relationship("ProductData", back_populates="additional_attributes")

        __mapper_args__ = {
            "polymorphic_identity": "attribute",
            "polymorphic_on": type,
        }
        
    class Capacity(Attribute):
        __tablename__ = "capacity"
        id = Column(Integer, ForeignKey("attribute.id", ondelete="CASCADE"), primary_key=True)
        unit = Column(String)
        item_count = Column(Integer)
        item_capacity = Column(Float)
        __mapper_args__ = {
            "polymorphic_identity": "capacity",
        }
        
    class CapacityUnknown(Attribute):
        __tablename__ = "capacity_unknown"
        id = Column(Integer, ForeignKey("attribute.id", ondelete="CASCADE"), primary_key=True)
        description = Column(String)
        __mapper_args__ = {
            "polymorphic_identity": "capacity_unknown",
        }
        
    class Set(Attribute):
        __tablename__ = "set"
        id = Column(Integer, ForeignKey("attribute.id", ondelete="CASCADE"), primary_key=True)
        item_count = Column(Integer)
        unit = Column(String)
        __mapper_args__ = {
            "polymorphic_identity": "set",
        }
        
    class OtherAttribute(Attribute):
        __tablename__ = "other_attribute"
        id = Column(Integer, ForeignKey("attribute.id", ondelete="CASCADE"), primary_key=True)
        description = Column(String)
        __mapper_args__ = {
            "polymorphic_identity": "other_attribute",
        }

    class Variant(Base):
        __tablename__ = "variant"
        id = Column(Integer, primary_key=True)
        type = Column(String)

        product_data_id = Column(Integer, ForeignKey("product_data.id", ondelete="CASCADE"), nullable=False)
        product_data = relationship("ProductData", back_populates="variants")
        
        values = relationship(
            "VariantValue",
            back_populates="variant",
            cascade="all, delete",
            passive_deletes=True,
        )

    class VariantValue(Base):
        __tablename__ = "variant_value"
        id = Column(Integer, primary_key=True)
        value = Column(String)

        variant_id = Column(Integer, ForeignKey("variant.id", ondelete="CASCADE"), nullable=False)
        variant = relationship("Variant", back_populates="values")

    class Category(Base):
        __tablename__ = "category"
        id = Column(Integer, primary_key=True)
        name = Column(String)

        parent_category_id = Column(Integer, ForeignKey("category.id", ondelete="CASCADE"))
        parent_category = relationship("Category", foreign_keys=[parent_category_id], backref="sub_categories", remote_side=id)
        
#        sub_categories = relationship(
#            "Category",
#            back_populates="parent_category",
#            cascade="all, delete",
#            passive_deletes=True,
#        )

        first_parent_category_id = Column(Integer, ForeignKey("category.id", ondelete="CASCADE"))
        first_parent_category = relationship("Category",foreign_keys=[first_parent_category_id], backref="all_sub_categories", remote_side=id)
        
#        all_sub_categories = relationship(
#            "Category",
#            back_populates="first_parent_category",
#            cascade="all, delete",
#            passive_deletes=True,
#        )
        
        product_data = relationship(
            "ProductData",
            back_populates="category"
        )

    engine = create_engine(
    f'postgresql://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{pg_db}'
    )

    Base.metadata.create_all(engine)
except Exception as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    exit()

logger.info("SUCCESS: Connection to RDS PostgreSQL instance succeeded")

def handler(event, context):
    """
    This function fetches content from PostgreSQL RDS instance
    """
    
    logger.info("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    logger.info("SUCCESS: Parsing of request succedeed ")
    
    try:
        s3_file = s3.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        logger.error(f'Error getting object {key} from bucket {bucket}. Make sure they exist and your bucket is in the same region as this function.')
        logger.error(e)
        raise e

    logger.info("SUCCESS: Retrievment of json from S3 succeeded")
    
    data = s3_file['Body'].read().decode('utf-8')
    json_data = json.loads(data)

    logger.info(json_data)
    
    with Session(engine, expire_on_commit=False) as session:

        # Getting 'Accueil'
        home_cat = session.query(Category).filter(Category.name == json_data["categories"][0]).first()
        if not home_cat:
            home_cat = Category(name=json_data["categories"][0], parent_category=None)
            session.add(home_cat)
            session.commit()
        
        # Getting the mother category
        main_cat = session.query(Category).filter(Category.name == json_data["categories"][1]).first()
        if not main_cat:
            main_cat = Category(name=json_data["categories"][1], parent_category=home_cat)
            session.add(main_cat)
            session.commit()
        previous_cat = main_cat

        actual_cat = None
        for loop_category in json_data["categories"][2:len(json_data["categories"])-1]:
            actual_cat = session.query(Category).filter(Category.name == loop_category).first()
            if not actual_cat:
                actual_cat = Category(name=loop_category, parent_category=previous_cat, first_parent_category=main_cat)
                session.add(actual_cat)
                session.commit()
            previous_cat = actual_cat
        
        product = session.get(Product, json_data["url"])
        if not product:
            product = Product(url=json_data["url"])
            session.add(product)
        
        product_data = ProductData(
            name=json_data["name"],
            availability=json_data["availability"],
            s3_path=json_data["s3_paths"]["item_path"],
            rating_count=json_data["rating_people_count"],
            rating_value=json_data["rating_value"] and json_data["rating_value"].replace(",", "."),
            brand=json_data["brand"],
            price_currency=json_data["currency"],
            price_unit=json_data["price"] and json_data["price"].replace(",", "."),
            price_base_value=json_data["base_price"] and json_data["base_price"]["value"].replace(",", "."),
            price_base_unit=json_data["base_price"] and json_data["base_price"]["unit"],
            shop=json_data["shop"],
            icon_alt=json_data["img"]["alt"],
            icon_src=json_data["img"]["src"],
            icon_s3_path=json_data["s3_paths"]["image_path"],
            timestamp=datetime.now(),
            product=product,
            category=actual_cat)
        session.add(product_data)

        for single_contenance in json_data["additional_attributes"]["single_contenances"]:
            attr = Capacity(unit=single_contenance["unit"], item_count=1, item_capacity=single_contenance["contenance"].replace(",", "."), product_data=product_data)
            session.add(attr)
        
        for multiple_contenance in json_data["additional_attributes"]["multiple_contenances"]:
            attr = Capacity(unit=multiple_contenance["unit"], item_count=multiple_contenance["nb"], item_capacity=multiple_contenance["contenance"].replace(",", "."), product_data=product_data)
            session.add(attr)
        
        for unkown_contenance in json_data["additional_attributes"]["unkown_contenances"]:
            attr = CapacityUnknown(description=unkown_contenance["contenance"], product_data=product_data)
            session.add(attr)
        
        for lot in json_data["additional_attributes"]["lots"]:
            attr = Set(item_count=lot["lot_count"], unit=lot["unit"], product_data=product_data)
            session.add(attr)
        
        for other_attribute in json_data["additional_attributes"]["unknown"]:
            attr = OtherAttribute(description=other_attribute, product_data=product_data)
            session.add(attr)

        for key, value in json_data["variants"].items():
            variant = Variant(type=key, product_data=product_data)
            session.add(variant)
            for val in value:
               variant_val = VariantValue(value=val, variant=variant)
               session.add(variant_val)

        session.commit()
