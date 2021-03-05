
from sqlalchemy import create_engine,MetaData,text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Text,Integer, String, Float,DateTime
from sqlalchemy.orm import sessionmaker, scoped_session
from flask import Flask,jsonify,request
from flask_marshmallow import Marshmallow

import datetime

from psycopg2 import sql

app = Flask(__name__)
app.debug=True
ma = Marshmallow(app)


Base = declarative_base()
def create_models(tablename):

    class Results(Base):

        __tablename__ = tablename
        __table_args__ = {'extend_existing': True}
        # __table_args__ = {'autoload': True}

        id = Column("id", Integer, primary_key=True,autoincrement=True)
        avg_bytes = Column("avg_bytes", Text, nullable=True)
        avg_latency = Column("avg_latency", Text, nullable=True)
        avg_connect_time = Column("avg_connect_time", Text, nullable=True)
        avg_response_time = Column("avg_response_time", Text, nullable=True)
        avg_throughput = Column("avg_throughput", Text, nullable=True)
        created_at = Column("created_at", DateTime, nullable=False, default=datetime.datetime.utcnow)
        concurrency = Column("concurrency", Float, nullable=True)
        duration = Column("duration", Float, nullable=True)
        fail_count = Column("fail_count", Float, nullable=True)
        perc_0 = Column("perc_0", Float, nullable=True)
        perc_50 = Column("perc_50", Float, nullable=True)
        perc_90 = Column("perc_90", Float, nullable=True)
        perc_95 = Column("perc_95", Float, nullable=True)
        perc_99 = Column("perc_99", Float, nullable=True)
        perc_99_9 = Column("perc_99_9", Float, nullable=True)
        perc_100 = Column("perc_100", Float, nullable=True)
        stdev_response_time = Column("stdev_response_time", Float, nullable=True)
        success = Column("success", Float, nullable=True)
        test_master_id = Column("test_master_id", String, nullable=True)
        test_id = Column("test_id", String , nullable=True)
        thresholds = Column("thresholds", Text, nullable=True)
        throughput = Column("throughput", Float, nullable=True)
        updated_at = Column("updated_at", DateTime, nullable=False, default=datetime.datetime.utcnow)

        
    return Results #return the class 
class ResultsSchema(ma.Schema):
    class Meta:
        fields = ("avg_bytes" ,"avg_latency" ,"avg_connect_time" ,"avg_response_time" ,"avg_throughput"  ,"concurrency ","duration" ,"fail_count" ,"perc_0" ,"perc_50 ","perc_90","perc_95" ,"perc_99" ,"perc_99_9" ,"perc_100" ,"stdev_response_time", "success"  ,"test_master_id"  ,"test_id"  , "thresholds"  ,"throughput" )


post_schema = ResultsSchema()
posts_schema = ResultsSchema(many=True)


@app.route('/result/<string:tbl_id>',methods=['POST'])
def post_data(tbl_id):
	Results = create_models(tbl_id)

	engine = create_engine('postgresql://postgres:pass@localhost/results')
	Base.metadata.create_all(bind=engine)

	session_factory = sessionmaker(bind=engine)
	Session = scoped_session(session_factory)()

	res=Results(
            
            avg_bytes=request.json['avg_bytes'], 
            avg_latency =request.json['avg_latency'],            
            avg_connect_time =request.json['avg_connect_time'],
            avg_response_time =request.json['avg_response_time'],            
            avg_throughput =request.json['avg_throughput'],
                        
            concurrency =request.json['concurrency'],                  
            duration  =request.json['duration'],              
            fail_count =request.json['fail_count'],             
            perc_0 =request.json['perc_0'],                 
            perc_50 =request.json['perc_50'],                
            perc_90 =request.json['perc_90'],                
            perc_95=request.json['perc_95'],                       
            perc_99 =request.json['perc_99'],
            perc_99_9=request.json['perc_99_9'], 
            perc_100 =request.json['perc_100'],
            stdev_response_time=request.json['stdev_response_time'], 
            success =request.json['success'],
            test_master_id=request.json['test_master_id'], 
            test_id =request.json['test_id'],
            thresholds =request.json['thresholds'],
            throughput =request.json['throughput'],
            
		)
	Session.merge(res)
	Session.commit()

	return post_schema.dump(res)


@app.route('/result/<string:tbl_id>/<string:test_id>/<string:test_master_id>',methods=['GET'])
def get_data(tbl_id,test_id,test_master_id):


    engine = create_engine("postgresql://postgres:pass@localhost/results", client_encoding="UTF-8")



    result = engine.execute(text("SELECT * FROM {0}  T WHERE T.test_id='{1}' and T.test_master_id='{2}'  ".format(tbl_id,str(test_id),str(test_master_id))))

    teamdata=[]
    for _r in result:
        teamdata.append(dict(_r))
    return jsonify(teamdata)
@app.route('/result/<string:tbl_id>/<string:test_id>/<string:test_master_id>',methods=['DELETE'])
def delete_data(tbl_id,test_id,test_master_id):


    engine = create_engine("postgresql://postgres:pass@localhost/results", client_encoding="UTF-8")


    result = engine.execute(text("DELETE  FROM {0}  T WHERE T.test_id='{1}' and T.test_master_id='{2}'  ".format(tbl_id,str(test_id),str(test_master_id))))

    if result:
        return jsonify({"message":"data deleted"},200)
    else:
        return jsonify({"error": "Collection not found"}, 404)



if __name__ == '__main__':
    app.run()