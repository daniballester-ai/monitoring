from flask import Flask, jsonify

app = Flask(__name__)

# Dados de exemplo (poderiam vir de um banco de dados ou outra fonte)
example_data = {
    "time": "00h 10",
    "status": "approved",
    "count": 120
}

@app.route('/api/get_record', methods=['GET'])
def get_record():
    return jsonify(example_data)

if __name__ == '__main__':
    app.run(debug=True)