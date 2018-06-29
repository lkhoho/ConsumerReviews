from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

mysql_engine = create_engine('mysql+pymysql://root:welcome@localhost:3306/CONSUMER_REVIEWS?charset=utf8mb4',
                             echo=False)
MySqlSession = sessionmaker(bind=mysql_engine)
