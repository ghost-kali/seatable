from flask import Flask, request

app = Flask(__name__)

@app.route('/quiz', methods=['POST'])
def get_quiz():
    # print raw body
    print("Raw body:", request.data.decode('utf-8'))

    # or if JSON is expected
    try:
        print("Parsed JSON:", request.get_json())
    except:
        pass

    return "OK", 200

if __name__ == '__main__':
    app.run(debug=True)
