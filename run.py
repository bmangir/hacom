from flask import Flask

from db.mongo import MongoConnector
from db.postgres import PostgresConnector
from recommendations.content_based.recommend import content_based_blueprint

PostgresConnector.initialize_connection_pool()
postgresSQL_conn = PostgresConnector.get_connection()
PostgresConnector.close_all_connections()

# Initialize the MongoDB connector instance
mongo_connector = MongoConnector()
mongodb_client = mongo_connector.get_client()


app = Flask(__name__)

app.register_blueprint(content_based_blueprint)


@app.route("/")
@app.route("/main")
def index():
    return {"Message": "Successful"}


if __name__ == '__main__':
    app.run(host="127.0.0.1", port=8080, debug=True)
