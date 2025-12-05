from flask import Flask, request
from flask_cors import CORS

from main import ai_sql

app = Flask(__name__)
CORS(app)   # Enable CORS

@app.post("/webhook")
def webhook():
    body = request.get_json()          # parsed JSON body
    print("Body:", body)
    response = ai_sql(body)
    print(response)
    return response

if __name__ == "__main__":
    app.run(debug=True)
