from flask import Flask, request, jsonify
import suggestion_service


app = Flask(__name__)


@app.route('/index-suggestion', methods=['GET'])
def get_suggestions():
    try:
        tenant_id = request.args.get('tenant_id')
        document_name = request.args.get('document_name')
        
        if not tenant_id or not document_name:
            return invalid_request()
        
        suggestion = suggestion_service.get_suggestion(tenant_id, document_name)

        return success(suggestion)
        
    except Exception as e:
        return internal_server_response(e)
    

def internal_server_response(error_msg):
        return jsonify({
        "data": [],
        "status_code": 1,
        "message": f"An error occurred: {str(error_msg)}"
    }), 500


def invalid_request():
    return jsonify({
        "data": [],
        "status_code": 1,
        "message": "Invalid query parameters"
    }), 400


def success(data):
    return jsonify({
        "data": data,
        "status_code": 0,
        "message": "Get suggestion successfully"
    }), 200
