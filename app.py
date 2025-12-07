from flask import Flask, request
from flask_cors import CORS

from main import ai_sql

app = Flask(__name__)
CORS(app)   # Enable CORS

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        print("OK")
        return {"message": "Ping OK"}

    body = request.get_json()
    print("Body:", body)
    response = ai_sql(body)
    print(response)
    return response

if __name__ == "__main__":
    app.run(debug=True)
