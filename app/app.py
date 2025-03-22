from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import os
from dotenv import load_dotenv
from Avg_Timezone_optimized import process_all_zips
import json
import asyncio

# Load environment variables
load_dotenv()

app = Flask(__name__)
CORS(app)

# Cache for storing results
results_cache = {
    'processing': False,
    'last_run': None,
    'error': None
}

@app.route('/')
def index():
    """Render the main page."""
    return render_template('index.html')

@app.route('/api/process', methods=['POST'])
def start_processing():
    """Start the sunset time processing."""
    global results_cache
    
    if results_cache['processing']:
        return jsonify({
            'status': 'error',
            'message': 'Processing is already in progress'
        }), 409
    
    try:
        results_cache['processing'] = True
        results_cache['error'] = None
        
        # Run the processing asynchronously
        asyncio.run(process_all_zips())
        
        # Load the results
        with open('sunset_summary.json', 'r') as f:
            results = json.load(f)
            
        results_cache['last_run'] = results
        results_cache['processing'] = False
        
        return jsonify({
            'status': 'success',
            'data': results
        })
        
    except Exception as e:
        results_cache['error'] = str(e)
        results_cache['processing'] = False
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/status')
def get_status():
    """Get the current processing status."""
    return jsonify({
        'processing': results_cache['processing'],
        'error': results_cache['error'],
        'has_results': results_cache['last_run'] is not None
    })

@app.route('/api/results')
def get_results():
    """Get the latest processing results."""
    if not results_cache['last_run']:
        return jsonify({
            'status': 'error',
            'message': 'No results available'
        }), 404
        
    return jsonify({
        'status': 'success',
        'data': results_cache['last_run']
    })

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True) 