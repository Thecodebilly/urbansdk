from flask import Flask, jsonify, request
from flask_restful import Resource, Api
import db

app = Flask(__name__)

api = Api(app)

class Hello(Resource):
  

    def get(self):
        
        return jsonify({'message': 'hello world'})

    def post(self):
          
        data = request.get_json()   
        return jsonify({'data': data})
  
class States(Resource):
  
    def get(self):
        rows=db.query_states()
        return jsonify({'number of states':len(rows)})
class Counties(Resource):
  
    def get(self):
        rows=db.query_counties()
        return jsonify({'number of counties':len(rows)})
        #return jsonify({'query results':rows,
        #                'number of counties':len(rows)})
  
    def post(self):
          
        data = request.get_json()     
        return jsonify({'data': data}) 


api.add_resource(Hello, '/')
api.add_resource(Counties, '/Counties')
api.add_resource(States, '/States')


if __name__ == '__main__':
  
    app.run(debug = True)