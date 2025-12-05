from flask import Flask, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)   # Enable CORS

@app.post("/webhook")
def webhook():
    body = request.get_json()          # parsed JSON body
    print("Body:", body)               # print the body
    return "OK", 200

if __name__ == "__main__":
    app.run(debug=True)
