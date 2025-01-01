from flask import Flask, render_template, send_from_directory

from db.mongo import MongoConnector
from db.postgres import PostgresConnector
from controller.user_controller import user_blueprint
from controller.main_controller import main_controller_blueprint

#PostgresConnector.initialize_connection_pool()
#postgresSQL_conn = PostgresConnector.get_connection()
#PostgresConnector.close_all_connections()
#
## Initialize the MongoDB connector instance
#mongo_connector = MongoConnector()
#mongodb_client = mongo_connector.get_client()


app = Flask(__name__)
app.register_blueprint(user_blueprint)
app.register_blueprint(main_controller_blueprint)

@app.route("/")
@app.route("/home")
def index():
    return render_template("main.html")


if __name__ == '__main__':
    app.run(host="127.0.0.1", port=8080, debug=True)
