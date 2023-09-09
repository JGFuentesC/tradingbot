import yfinance as yf
import pandas as pd
from flask import jsonify,request
import gcsfs 

def descargarAccion(stock:str)->pd.DataFrame:
    data = yf.download(stock, period="3mo")
    data.reset_index(inplace=True)
    data['Date'] = pd.to_datetime(data['Date'],errors='coerce').dt.date
    data.columns =['date','open','high','low','close','adjclose','volume']
    data.insert(0,'stock',stock)
    return data

def getStock(request):
    # Intentar obtener el payload en formato JSON
    try:
        request_data = request.get_json(silent=True)
        
        # Verificar si el payload es None o si no tiene el parámetro "stock"
        if not request_data or 'stock' not in request_data:
            return jsonify({
                'error': 'The payload must contain a "stock" parameter.'
            }), 400
        
        # Validar que "stock" sea de tipo string
        stock_value = request_data['stock']
        if not isinstance(stock_value, str):
            return jsonify({
                'error': 'The "stock" parameter must be of type string.'
            }), 400

        # Si todo está correcto, descargar los datos de la acción y escribe el resultado en GCS
        data = descargarAccion(stock_value)
        data.to_parquet(f'gs://m5g18/stocks/{stock_value}.parquet', index=False)
        return jsonify({
            'status': "SUCCESS"
        }), 200

    except Exception as e:
        return jsonify({'status':'ERROR',
            'error': f'Error processing the request: {str(e)}'
        }), 500