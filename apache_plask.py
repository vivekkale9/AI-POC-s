from flask import Flask
import os

from reportAPI import report_blueprint
from MasterBOAPI import masterbo_blueprint
from widgetAPI import widget_blueprint

app = Flask(__name__)



app.register_blueprint(masterbo_blueprint,url_prefix = '/a_spark')
    

    
app.register_blueprint(report_blueprint, url_prefix='/a_spark')
 


app.register_blueprint(widget_blueprint, url_prefix='/a_spark')

 

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 6000))
    app.run(host='0.0.0.0', port=port)

