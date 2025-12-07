from flask import Flask, request
from flask_cors import CORS

from main import ai_sql

app = Flask(__name__)
CORS(app)   # Enable CORS

@app.route("/webhook", methods=["HEAD", "GET", "POST"])
def webhook():
    # If HEAD request → trigger your job silently
    if request.method == "HEAD":
        print("HEAD ping received → Running scheduled job")
        ai_sql({})   # or pass default payload
        return "", 200

    # Normal POST flow
    if request.method == "POST":
        body = request.get_json()
        print("Body:", body)
        response = ai_sql(body)
        return response

    # GET fallback
    return {"message": "Webhook OK"}, 200


if __name__ == "__main__":
    app.run(debug=True)
