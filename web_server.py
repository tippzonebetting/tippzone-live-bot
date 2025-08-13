from flask import Flask
app = Flask(__name__)

@app.route('/')
def home():
    return "TippZone Live Bot Running"

@app.route('/health')
def health():
    return "OK"

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)

