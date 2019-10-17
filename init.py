from flask import Flask,redirect,url_for,request,render_template,jsonify,session,make_response
import os
from datetime import datetime
import psycopg2
import numpy as np
import csv
import pandas as pd
import pylab as plt
import matplotlib.pyplot as plt2
import matplotlib.pyplot as plt
from cStringIO import StringIO
from io import BytesIO
import base64
import urllib
import json
import plotly
import plotly.graph_objs as go
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext  # Import SQL modules
import pydotplus
from sklearn.datasets import load_iris
from sklearn import tree
import collections
from matplotlib import style
from sklearn.cluster import KMeans
#PEOPLE_FOLDER = os.path.join('/home/poorva/flask-application/static', 'PEOPLE_PHOTO')
app = Flask(__name__)
#app.config['UPLOAD_FOLDER']=PEOPLE_FOLDER
@app.route('/success')
def success():
    #full_filename = os.path.join(app.config['UPLOAD_FOLDER'], 'seaborn.png')
    graphJSON3=linechart_guh()
    graphJSON1=barchart_fine_guh()
    graphJSON2=barchart_fine_nas()
    graphJSON4=linechart_nas()
    graphJSON6=piechart()
    graphJSON5=piechart_guh()
    graphJSON7=timechart_guh()
    graphJSON8=timechart()
    graphJSON9=gender()
    graphJSON10=age()
    results=compare()
    cluster=clusteranalysis()
    return render_template('dashboard1.html',graphJSON1=graphJSON1,graphJSON3=graphJSON3,graphJSON2=graphJSON2,graphJSON4=graphJSON4,graphJSON5=graphJSON5,graphJSON6=graphJSON6,graphJSON7=graphJSON7,graphJSON8=graphJSON8,graphJSON9=graphJSON9,graphJSON10=graphJSON10,cluster=cluster,results=results)
def linechart_nas():
	sc = SparkContext.getOrCreate()
	sqlContext = SQLContext(sc)
    	plotly.tools.set_credentials_file(username='divekale16', api_key='bKoSx4rnYbB1zRmNOdK5')
	conn = psycopg2.connect("host=localhost dbname=punefulldb user=postgres")
	print "Connected"
	cur = conn.cursor()
	'''
	cur.execute("create table top1 as select t_challan_header_dtls.fk_offense_master,t_mst_offense.offense_name,t_challan_header_dtls.fk_challan_header from t_challan_header_dtls inner join t_mst_offense on t_challan_header_dtls.fk_offense_master=t_mst_offense.pk_id;")
	cur.execute("create table top2 as  select t_challan_header.offense_date,t_challan_header.offense_time,top1.offense_name,top1.fk_offense_master from t_challan_header inner join top1 on t_challan_header.pk_id=top1.fk_challan_header;")
	'''
	query="SELECT * FROM top2 LIMIT 1000"	
	outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)
	with open("newarea_nashik.csv", 'w') as f:
	    cur.copy_expert(outputquery, f)
	print "Connected"
	conn.close()
	# Create a dataframe of the violation type frequency
	violist=[]
	raw_rdd = sc.textFile("/home/apple/spark-1.6.3-bin-hadoop2.6/newarea_nashik.csv")\
	    .map(lambda line: line.split(","))
	raw_df = raw_rdd.toDF(['offense_date','offense_time','offense_name','fk_offense_master'])

	violations_df = raw_df.rollup('offense_name').count().sort('count',ascending=False)
	# Create a list with the top 10 violation's frequencies
	ViolationsTop10 = violations_df.select('count').rdd.flatMap(lambda y: y).collect()

	violist = violations_df.select('offense_name').collect()
	Total = raw_df.count()
	ViolationsTop10 = [ViolationsTop10[1], ViolationsTop10[2], ViolationsTop10[3], ViolationsTop10[4], ViolationsTop10[5]]

	# Plot a bar chart of top10 violations

	data1 = [go.Scatter(
		    x=[violist[1].offense_name,violist[2].offense_name,violist[3].offense_name,violist[4].offense_name,violist[5].offense_name],
		    y=ViolationsTop10,
				marker = dict(
		 	 color = 'green'
			)
	    )]

	layout = go.Layout(
	    title='Top 5 violations',
		xaxis=dict(
		title='Name of Violation',
		titlefont=dict(
		    family='Courier New, monospace',
		    size=18,
		    color='#7f7f7f'
		)
	    ),
	    yaxis=dict(
		title='Number of People causing it',
		titlefont=dict(
		    family='Courier New, monospace',
		    size=18,
		    color='#7f7f7f'
		)
	    )
	)
	graph4=dict(data=data1,layout=layout)

    	graphJSON4 = json.dumps(graph4, cls=plotly.utils.PlotlyJSONEncoder)
    	return graphJSON4
#finetrendguwahati
def barchart_fine_guh():
    	conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
	print "Connected"
	cur = conn.cursor()
	
	query="SELECT * FROM t_challan_header"

	outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)

	with open('finetrends.csv', 'w') as f:
	    cur.copy_expert(outputquery, f)

	print "Connected"
	conn.close()



	plotly.tools.set_credentials_file(username='divekale16', api_key='bKoSx4rnYbB1zRmNOdK5')

	raw_df=pd.DataFrame.from_csv("finetrends.csv")
	print raw_df
	a=[]
	print "length"
	print len(a)

	for row in raw_df.loc[:,"compounding_fee"] :
	    a.append(row)

	count1 =0
	count100_200=0
	count300_400=0
	count500_1000=0
	count1000_4000=0

	for i in range(1,len(a)):
		if a[i]>=1 and a[i]<100:
			count1=count1+1
		elif a[i]>=100 and a[i]<=200:
	       		count100_200=count100_200+1
		elif a[i]>=200 and a[i]<=500:
			count300_400=count300_400+1
		elif a[i]>=500 and a[i]<=1000:
			count500_1000=count500_1000+1
		elif a[i]>=1000 and a[i]<=4000:
			count1000_4000=count1000_4000+1


	p1=count1
	p2=count100_200
	p3=count300_400
	p4=count500_1000
	p5=count1000_4000


	data1 = [go.Bar(
		    x=["1-100", "100-200","200-500","500-1000","1000-4000"],
		    y=[p1,p2,p3,p4,p5],
			
	 			marker = dict(
		 	 color = 'red'
			),
			name = 'Scale=1unit/person'
	
	    )]

	layout = go.Layout(
	    title='Count of People vs Fine payed',
		xaxis=dict(
		title='Range of Fine',
		titlefont=dict(
		    family='Courier New, monospace',
		    size=18,
		    color='#7f7f7f'
		)
	    ),
	    yaxis=dict(
		title='Number of People',
		titlefont=dict(
		    family='Courier New, monospace',
		    size=18,
		    color='#7f7f7f'
		)
	    )
	)



	#fig = go.Figure(data=data1, layout=layout)
	#py.plot(fig,data1, filename='data')
	graph1=dict(data=data1,layout=layout)
    	graphJSON1 = json.dumps(graph1, cls=plotly.utils.PlotlyJSONEncoder)
    	return graphJSON1

def barchart_fine_nas():
    	conn = psycopg2.connect("host=localhost dbname=punefulldb user=postgres")
	print "Connected"
	cur = conn.cursor()
	
	query="SELECT * FROM t_challan_header"

	outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)

	with open('finetrends1.csv', 'w') as f:
	    cur.copy_expert(outputquery, f)

	print "Connected"
	conn.close()



	plotly.tools.set_credentials_file(username='divekale16', api_key='bKoSx4rnYbB1zRmNOdK5')

	raw_df=pd.DataFrame.from_csv("finetrends1.csv")
	print raw_df
	a=[]
	print "length"
	print len(a)

	for row in raw_df.loc[:,"compounding_fee"] :
	    a.append(row)

	count1 =0
	count100_200=0
	count300_400=0
	count500_1000=0
	count1000_4000=0

	for i in range(1,len(a)):
		if a[i]>=1 and a[i]<100:
			count1=count1+1
		elif a[i]>=100 and a[i]<=200:
	       		count100_200=count100_200+1
		elif a[i]>=200 and a[i]<=500:
			count300_400=count300_400+1
		elif a[i]>=500 and a[i]<=1000:
			count500_1000=count500_1000+1
		elif a[i]>=1000 and a[i]<=4000:
			count1000_4000=count1000_4000+1


	p1=count1
	p2=count100_200
	p3=count300_400
	p4=count500_1000
	p5=count1000_4000


	data1 = [go.Bar(
		    x=["1-100", "100-200","200-500","500-1000","1000-4000"],
		    y=[p1,p2,p3,p4,p5],
			
	 			marker = dict(
		 	 color = 'red'
			),
			name = 'Scale=1unit/person'
	
	    )]

	layout = go.Layout(
	    title='Count of People vs Fine payed',
		xaxis=dict(
		title='Range of Fine',
		titlefont=dict(
		    family='Courier New, monospace',
		    size=18,
		    color='#7f7f7f'
		)
	    ),
	    yaxis=dict(
		title='Number of People',
		titlefont=dict(
		    family='Courier New, monospace',
		    size=18,
		    color='#7f7f7f'
		)
	    )
	)



	#fig = go.Figure(data=data1, layout=layout)
	#py.plot(fig,data1, filename='data')
	graph2=dict(data=data1,layout=layout)
    	graphJSON2 = json.dumps(graph2, cls=plotly.utils.PlotlyJSONEncoder)
    	return graphJSON2

def linechart_guh():
	
	sc = SparkContext.getOrCreate()
	sqlContext = SQLContext(sc)
    	plotly.tools.set_credentials_file(username='divekale16', api_key='bKoSx4rnYbB1zRmNOdK5')
	conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
	print "Connected"
	cur = conn.cursor()
	
	query="SELECT * FROM top2 LIMIT 1000"	
	outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)
	with open("newarea_guh.csv", 'w') as f:
	    cur.copy_expert(outputquery, f)
	print "Connected"
	conn.close()
	# Create a dataframe of the violation type frequency
	violist=[]
	raw_rdd = sc.textFile("/home/apple/spark-1.6.3-bin-hadoop2.6/newarea_guh.csv")\
	    .map(lambda line: line.split(","))
	raw_df = raw_rdd.toDF(['offense_date','offense_time','offense_name','fk_offense_master'])

	violations_df = raw_df.rollup('offense_name').count().sort('count',ascending=False)
	# Create a list with the top 10 violation's frequencies
	ViolationsTop10 = violations_df.select('count').rdd.flatMap(lambda y: y).collect()

	violist = violations_df.select('offense_name').collect()
	Total = raw_df.count()
	ViolationsTop10 = [ViolationsTop10[1], ViolationsTop10[2], ViolationsTop10[3], ViolationsTop10[4], ViolationsTop10[5]]

	# Plot a bar chart of top10 violations

	data1 = [go.Scatter(
		    x=[violist[1].offense_name,violist[2].offense_name,violist[3].offense_name,violist[4].offense_name,violist[5].offense_name],
		    y=ViolationsTop10,
				marker = dict(
		 	 color = 'blue'
			)
	    )]

	layout = go.Layout(
	    title='Top 5 violations',
		xaxis=dict(
		title='Name of Violation',
		titlefont=dict(
		    family='Courier New, monospace',
		    size=18,
		    color='#7f7f7f'
		)
	    ),
	    yaxis=dict(
		title='Number of People causing it',
		titlefont=dict(
		    family='Courier New, monospace',
		    size=18,
		    color='#7f7f7f'
		)
	    )
	)
	graph3=dict(data=data1,layout=layout)

    	graphJSON3 = json.dumps(graph3, cls=plotly.utils.PlotlyJSONEncoder)
    	return graphJSON3
#area wise violations Nashik
def piechart():
	plotly.tools.set_credentials_file(username='divekale16', api_key='bKoSx4rnYbB1zRmNOdK5')

	sc = SparkContext.getOrCreate()
	sqlContext = SQLContext(sc)


	conn = psycopg2.connect("host=localhost dbname=punefulldb user=postgres")
	print "Connected"
	cur = conn.cursor()
	cur.execute("SELECT pk_id,location_code FROM t_challan_header ;")	#select time column name
	one = cur.fetchone()
	all = cur.fetchall()
	print one

	query="SELECT pk_id,location_code FROM t_challan_header"

	outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)

	with open('descriptive_area.csv', 'w') as f:
	    cur.copy_expert(outputquery, f)

	print "Connected"
	conn.close()


	raw_rdd = sc.textFile("/home/apple/spark-1.6.3-bin-hadoop2.6/descriptive_area.csv")\
	    .map(lambda line: line.split(","))


	raw_df = raw_rdd.toDF(['pk_id','location_code'])
	raw_df.show()


	# Calculate #violations for each month
	Kot = raw_df.filter(raw_df['location_code'] == '01').count()
	Era = raw_df.filter(raw_df['location_code'] == '04').count()
	Vim = raw_df.filter(raw_df['location_code'] == '05').count()
	Cam = raw_df.filter(raw_df['location_code'] == '06').count()
	Swa = raw_df.filter(raw_df['location_code'] == '07').count()

	Total = raw_df.count()
	# Plot months vs violations percentage
	#Area = [1,2,3,4,5]
	#Violations = [Kot,Era,Vim,Cam,Swa]

	labels=["Location 01", "Location 04", "Location 05", "Location 06", "Location 07"]
	values=[Kot,Era,Vim,Cam,Swa]
	
	trace=go.Pie(labels=labels,values=values)
    #layout=dict(title="violations by vehicle class")
        data=[trace]
        graph6=dict(data=data)
        graphJSON6 = json.dumps(graph6, cls=plotly.utils.PlotlyJSONEncoder)
        return graphJSON6
def piechart_guh():
	plotly.tools.set_credentials_file(username='divekale16', api_key='bKoSx4rnYbB1zRmNOdK5')

	sc = SparkContext.getOrCreate()
	sqlContext = SQLContext(sc)


	conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
	print "Connected"
	cur = conn.cursor()
	cur.execute("SELECT pk_id,location_code FROM t_challan_header ;")	#select time column name
	one = cur.fetchone()
	all = cur.fetchall()
	print one

	query="SELECT pk_id,location_code FROM t_challan_header"

	outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)

	with open('descriptive_area1.csv', 'w') as f:
	    cur.copy_expert(outputquery, f)

	print "Connected"
	conn.close()


	raw_rdd = sc.textFile("/home/apple/spark-1.6.3-bin-hadoop2.6/descriptive_area1.csv")\
	    .map(lambda line: line.split(","))


	raw_df = raw_rdd.toDF(['pk_id','location_code'])
	raw_df.show()


	# Calculate #violations for each month
	Kot = raw_df.filter(raw_df['location_code'] == '01').count()
	Era = raw_df.filter(raw_df['location_code'] == '03').count()
	Vim = raw_df.filter(raw_df['location_code'] == '05').count()
	#Cam = raw_df.filter(raw_df['location_code'] == '06').count()
	#Swa = raw_df.filter(raw_df['location_code'] == '07').count()

	Total = raw_df.count()
	# Plot months vs violations percentage
	#Area = [1,2,3,4,5]
	Violations = [Kot,Era,Vim]

	data1 = [go.Scatter(
		    x=["Location 01", "Location 03", "Location 05"],
		    y=Violations,
				marker = dict(
		 	 color = 'cyan'
			)
	    )]

	layout = go.Layout(
	    title='Area wise Violation',
		xaxis=dict(
		title='Violation Area code',
		titlefont=dict(
		    family='Courier New, monospace',
		    size=18,
		    color='#7f7f7f'
		)
	    ),
	    yaxis=dict(
		title='Number of violations',
		titlefont=dict(
		    family='Courier New, monospace',
		    size=18,
		    color='#7f7f7f'
		)
	    )
	)
        graph5=dict(data=data1,layout=layout)
        graphJSON5 = json.dumps(graph5, cls=plotly.utils.PlotlyJSONEncoder)
        return graphJSON5
#time wise violations for nashik
def timechart():
	plotly.tools.set_credentials_file(username='divekale16', api_key='bKoSx4rnYbB1zRmNOdK5')

	date_format = "%H:%M:%S.%f"

	
	conn = psycopg2.connect("host=localhost dbname=punefulldb user=postgres")
	print "Connected"
	cur = conn.cursor()
	cur.execute("SELECT pk_id,offense_time FROM t_challan_header ;")	#select time column name
	one = cur.fetchone()
	all = cur.fetchall()
	print one

	query="SELECT pk_id,offense_time FROM t_challan_header"

	outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)

	with open('table.csv', 'w') as f:
	    cur.copy_expert(outputquery, f)

	print "Connected"
	conn.close()


	raw_df=pd.DataFrame.from_csv("table.csv")
	print raw_df
	a=[]

	a2=[]
	for row in raw_df.loc[:,'offense_time'] :
		a2.append(row)
	print len(a2)

	a=[i.split(':', 1)[0] for i in a2]



	count1=0
	count2=0
	count3=0
	count4=0

	for i in range(1,len(a)):
		if int(a[i])>=0 and int(a[i])<=6:
			count1=count1+1	
		elif int(a[i])>=6 and int(a[i])<=12:
		    	count2=count2+1
		elif int(a[i])>=12 and int(a[i])<=18:
			count3=count3+1
		elif int(a[i])>=18 and int(a[i])<=24:
			count4=count4+1


	timefile = [go.Bar(
            x=[count1,count2,count3,count4],
		    y=["12am-6am", "6am-12pm","12pm-6pm","6pm-12am"],
		    orientation = 'h'
	    )]

	layout = go.Layout(
	    title='No. of Violations VS Time',
		xaxis=dict(
		title='Time Range',
		titlefont=dict(
		    family='Courier New, monospace',
		    size=18,
		    color='#7f7f7f'
		)
	    ),
	    yaxis=dict(
		title='Number of Violations',
		titlefont=dict(
		    family='Courier New, monospace',
		    size=18,
		    color='#7f7f7f'
		)
	    )
	)
	graph8=dict(data=timefile,layout=layout)
	graphJSON8 = json.dumps(graph8, cls=plotly.utils.PlotlyJSONEncoder)
    	return graphJSON8
def timechart_guh():
	plotly.tools.set_credentials_file(username='divekale16', api_key='bKoSx4rnYbB1zRmNOdK5')

	date_format = "%H:%M:%S.%f"

	
	conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
	print "Connected"
	cur = conn.cursor()
	cur.execute("SELECT pk_id,offense_time FROM t_challan_header ;")	#select time column name
	one = cur.fetchone()
	all = cur.fetchall()
	print one

	query="SELECT pk_id,offense_time FROM t_challan_header"

	outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)

	with open('table1.csv', 'w') as f:
	    cur.copy_expert(outputquery, f)

	print "Connected"
	conn.close()


	raw_df=pd.DataFrame.from_csv("table1.csv")
	print raw_df
	a=[]

	a2=[]
	for row in raw_df.loc[:,'offense_time'] :
		a2.append(row)
	print len(a2)

	a=[i.split(':', 1)[0] for i in a2]



	count1=0
	count2=0
	count3=0
	count4=0

	for i in range(1,len(a)):
		if int(a[i])>=0 and int(a[i])<=6:
			count1=count1+1	
		elif int(a[i])>=6 and int(a[i])<=12:
		    	count2=count2+1
		elif int(a[i])>=12 and int(a[i])<=18:
			count3=count3+1
		elif int(a[i])>=18 and int(a[i])<=24:
			count4=count4+1


	timefile = [go.Bar(
			x=[count1,count2,count3,count4],
		    y=["12am-6am", "6am-12pm","12pm-6pm","6pm-12am"],
		    orientation = 'h'
	    )]

	layout = go.Layout(
	    title='No. of Violations VS Time',
		xaxis=dict(
		title='Time Range',
		titlefont=dict(
		    family='Courier New, monospace',
		    size=18,
		    color='#7f7f7f'
		)
	    ),
	    yaxis=dict(
		title='Number of Violations',
		titlefont=dict(
		    family='Courier New, monospace',
		    size=18,
		    color='#7f7f7f'
		)
	    )
	)
	graph7=dict(data=timefile,layout=layout)
	graphJSON7 = json.dumps(graph7, cls=plotly.utils.PlotlyJSONEncoder)
    	return graphJSON7
#compare trends for both cities
def compare():
	sc = SparkContext.getOrCreate()
	sqlContext = SQLContext(sc)

	# Create rdd of dataset

	conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
	print "Connected"
	cur = conn.cursor()

	query="SELECT * from dsp1 WHERE offense_date BETWEEN '2017-11-01' AND '2017-11-30'"

	outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)

	with open('comparison1.csv', 'w') as f:
	    cur.copy_expert(outputquery, f)

	query="SELECT * from dsp1 WHERE offense_date BETWEEN '2017-12-01' AND '2017-12-30'"

	outputquery1 = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)

	with open('comparison2.csv', 'w') as f1:
	    cur.copy_expert(outputquery1, f1)

	print "Connected"
	conn.close()

	#Make dataframes 
	date1 = sc.textFile("/home/apple/spark-1.6.3-bin-hadoop2.6/comparison1.csv")\
	    .map(lambda line: line.split(","))
	date2 = sc.textFile("/home/apple/spark-1.6.3-bin-hadoop2.6/comparison2.csv")\
	    .map(lambda line: line.split(","))

	date1df = date1.toDF(['offense_date','offense_time','offense_name','fk_offense_master'])	#made a dataframe
	date2df = date2.toDF(['offense_date','offense_time','offense_name','fk_offense_master'])

	violations1_df = date1df.rollup('offense_name').count().sort('count',ascending=False)			#got name of offense and its count in descending order
	violations2_df = date2df.rollup('offense_name').count().sort('count',ascending=False)


	Violations1Top10 = violations1_df.select('count').rdd.flatMap(lambda y: y).collect()	#got just the count
	Violations2Top10 = violations2_df.select('count').rdd.flatMap(lambda y: y).collect()


	nov=[]
	dec=[]
	common=[]
	common1=[]
	file1=[]
	namesnov=[]
	namesdec=[]
	file2=[]
	Total1 = date1df.count()
	Total2 = date2df.count()
	nov = violations1_df.select('offense_name').rdd.flatMap(lambda y: y).collect()	#got just the name
	dec = violations2_df.select('offense_name').rdd.flatMap(lambda y: y).collect()

	common=list(set(nov).intersection(dec))

	print common
	common.remove('offense_name')


	for i in range(0,len(common)):
		if((date1df.filter(date1df['offense_name'] == common[i])) and (date2df.filter(date2df['offense_name'] == common[i]))):
			cou=date1df.filter(date1df['offense_name'] == common[i]).count()
			cou1=date2df.filter(date2df['offense_name'] == common[i]).count()
			file1.append(cou)
			file2.append(cou1)

	l1=[]

	#l.append(v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,v14,v15,v16,v17,v18,v19,v20,v21,v22,v23,v24,v25,v26,v27,v28,v29,v30)

	for i in range(0,len(common)):
		l1.append([file1[i],file2[i]])
	print "*******************************"
	print l1[1]
	print len(l1)
	col=['black','blue','red','green','yellow','purple','orange','brown','cyan','magenta']
	LABELS = ["November","December"]
	plt.figure()
	Type=[1,2]
	for i in range(0,8):
		plt2.plot(Type,l1[i],color=col[i],label=common[i])
	plt2.legend()
	plt2.xlabel('MONTHS')
	plt2.ylabel('Count')
	plt2.xticks(Type,LABELS)
	plt2.title('Python Line Chart: Violations in different Months')
	#plt.show()
	figfile = BytesIO()
    	plt2.savefig(figfile, format='png')
    	figfile.seek(0)  # rewind to beginning of file
    	figdata_png = base64.b64encode(figfile.getvalue())
    	#figdata_png = urllib.quote(base64.b64encode(figfile.read()).decode())
    	return figdata_png
def gender():
	sc = SparkContext.getOrCreate()
	sqlContext = SQLContext(sc)
	raw_rdd = sc.textFile("/home/apple/spark-1.6.3-bin-hadoop2.6/data.csv").map(lambda line: line.split(","))
	raw_df = raw_rdd.toDF(['pkid','Violation','Gender','Licence','Violation Area','Age'])
	raw_df.show()
	# Calculate gender violations
	males = raw_df.filter(raw_df['Gender'] == 'M').count()
	females = raw_df.filter(raw_df['Gender'] == 'F').count()
	unknown = raw_df.count() - (males + females)
	# Plot the chart
	labels = ['Male', 'Female', 'Unknown']
	values = [males, females, unknown]
	trace=go.Pie(labels=labels,values=values)
	#layout=dict(title="violations by vehicle class")
	data=[trace]
	graph9=dict(data=data)
	graphJSON9 = json.dumps(graph9, cls=plotly.utils.PlotlyJSONEncoder)
	return graphJSON9
def age():
	raw_df=pd.DataFrame.from_csv("/home/apple/spark-1.6.3-bin-hadoop2.6/onlyage.csv")
	a=[]
	print "length"
	print len(a)
	for row in raw_df.loc[:,"Age"] :
		a.append(row)
	count1 =0
	count100_200=0
	count300_400=0
	count500_1000=0
	count1000_4000=0
	for i in range(1,len(a)):
		if a[i]>=1 and a[i]<20:
		    	count1=count1+1
		elif a[i]>=20 and a[i]<=40:
		   		count100_200=count100_200+1
		elif a[i]>=40 and a[i]<=60:
			count300_400=count300_400+1
		elif a[i]>=60 and a[i]<=80:
			count500_1000=count500_1000+1
		elif a[i]>=80 and a[i]<=100:
			count1000_4000=count1000_4000+1
	p1=count1
	p2=count100_200
	p3=count300_400
	p4=count500_1000
	p5=count1000_4000
	data1 = [go.Scatter(
		        x=["1-20 years","20-40 years","40-60 years","60-80 years","80-90 years"],
		        y=[p1,p2,p3,p4,p5],
			
	 			marker = dict(
		     	 color = 'blue'
		    	),
		    	name = 'Scale=1unit/person'
	
		)]

	layout = go.Layout(
		title='Count of Violations VS Age range of offenders',
		xaxis=dict(
		    title='Count of violations',
		    titlefont=dict(
		        family='Courier New, monospace',
		        size=18,
		        color='#7f7f7f'
		    )
		),
		yaxis=dict(
		    title='Age Range of offenders',
		    titlefont=dict(
		        family='Courier New, monospace',
		        size=18,
		        color='#7f7f7f'
		    )
		)
	)
	graph10=dict(data=data1,layout=layout)
	graphJSON10 = json.dumps(graph10, cls=plotly.utils.PlotlyJSONEncoder)
	return graphJSON10
def clusteranalysis():
	style.use("ggplot")
	raw_df=pd.DataFrame.from_csv("/home/apple/spark-1.6.3-bin-hadoop2.6/cluster.csv")
	X=[[]]
	print "come here"
	df = pd.read_csv('cluster.csv', usecols=['latitude','longitude'])	
	X=df.values
	#print X
	kmeans = KMeans(n_clusters=4)
	kmeans.fit(X)
	centroids = kmeans.cluster_centers_
	labels = kmeans.labels_
	print"Centroids are :"
	print(centroids)
	print "Labels are :"
	print(labels)	
	colors = ["g.","r.","c.","y."]
	plt.figure()
	for i in range(len(X)):
	   # print("coordinate:",X[i], "label:", labels[i])
		plt.plot(X[i][0], X[i][1], colors[labels[i]], markersize = 9)
	plt.scatter(centroids[:, 0],centroids[:, 1],marker='x', s=50, linewidths = 5, zorder = 10)
	figfile1 = BytesIO()
	plt.savefig(figfile1, format='png')
	figfile1.seek(0)  # rewind to beginning of file
	figdata_png1 = base64.b64encode(figfile1.getvalue())
	#figdata_png = urllib.quote(base64.b64encode(figfile.read()).decode())
	return figdata_png1
@app.route('/' , methods=['POST','GET'])
def login(): 
    if request.method == 'POST':
        user=request.form['username']
        session['username'] = request.form['username']
        if request.form['username'] == 'admin' and request.form['password'] == 'password':
            return redirect(url_for('success'))   
        
        else:
            return render_template('newlogin.html')
    elif request.method=='GET':
        return render_template('newlogin.html')
    else:
        return render_template('newlogin.html')
@app.route('/login' , methods=['POST','GET'])
def login1(): 
    if request.method == 'POST':
        user=request.form['username']
        session['username'] = request.form['username']
        if request.form['username'] == 'admin' and request.form['password'] == 'password':
            return redirect(url_for('success'))   
        
        else:
            return render_template('newlogin.html')
    elif request.method=='GET':
        return render_template('newlogin.html')
    else:
        return render_template('newlogin.html')
@app.route('/logout')
def logout():
   # remove the username from the session if it is there
   session.pop('username', None)
   return redirect(url_for('login'))  
   #return render_template('newlogin.html')
#@csrf.exempt
@app.route('/predictive',methods=['GET','POST'])
def predictive():
   if request.method == 'POST':
	day=request.form['Day']
   	time=request.form['Time']
	violation=request.form['Violation']
	#if day and time:   
	#	return jsonify({'day':newday})
        #return jsonify({'error':'missing data'})
	return redirect(url_for('result',day=day,time=time,violation=violation))
   else: 
   	return render_template('predictive.html')
@app.route('/result/<day>/<time>/<violation>')   
def result(day,time,violation):  
   if day and time:
	resultpred=analysis(day,time,violation)	#return redirect(url_for('result',day=newday))
	if resultpred==1:
		resultpred="Panchavati"
	elif resultpred==2:
		resultpred="Shivaji Nagar"
	elif resultpred==3:
		resultpred="Gandhi Nagar"
	elif resultpred==4:
		resultpred="Pakhal Road"
	elif resultpred==5:
		resultpred="MG Road"
	elif resultpred==6:
		resultpred="Indira Nagar"
	else:
		resultpred="College Road"
	#newday=trial()
	return render_template('result.html',resultpred=resultpred)
def analysis(day,time,violation):
	date_format = "%H:%M:%S.%f"
	# Create rdd of dataset

	conn = psycopg2.connect("host=localhost dbname=punefulldb user=postgres")
	print "Connected"
	cur = conn.cursor()

	query="SELECT * from pred LIMIT 20000"

	outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)

	with open('predictive_area.csv', 'w') as f:
		cur.copy_expert(outputquery, f)

	print "Connected"
	conn.close()


	raw_df=pd.read_csv("predictive_area.csv")


	a=[]
	a=['new_day','new_time','fk_offense_master']
	print a	#data

	b=[]
	for row in raw_df.loc[:,"location_code"] :
		b.append(row)

	print b	#we got Y=b

	#We will have to create lists for day,time


	date = pd.to_datetime(raw_df.offense_date)
	raw_df['weekday'] = date.dt.weekday

	a1=[]
	a1=raw_df['weekday']
	print a1	#has day of the week


	raw_df['new_day'] = a1
	raw_df.to_csv('predictive_area.csv')

	a2=[]
	for row in raw_df.loc[:,'offense_time'] :
		a2.append(row)
	print len(a2)

	a3=[i.split(':', 1)[0] for i in a2]
	print a3	#has time
	print len(a3)

	raw_df['new_time'] = a3
	raw_df.to_csv('predictive_area.csv')


	df = pd.read_csv('predictive_area.csv', usecols=['new_day','new_time','fk_offense_master'])
	c=[[]]
	c=df.values
	#we got X=c

	# Training
	clf = tree.DecisionTreeClassifier()
	clf = clf.fit(c,b)

	# Visualize data
	dot_data = tree.export_graphviz(clf,
		                            feature_names=a,
		                            out_file=None,
		                            filled=True,
		                            rounded=True)
	graph = pydotplus.graph_from_dot_data(dot_data)

	colors = ('turquoise', 'orange','red','yellow')
	edges = collections.defaultdict(list)


	for edge in graph.get_edge_list():
		edges[edge.get_source()].append(int(edge.get_destination()))

	for edge in edges:
		edges[edge].sort()    
		for i in range(2):
		    dest = graph.get_node(str(edges[edge][i]))[0]
		    dest.set_fillcolor(colors[i])
	print "Writitng"
	#graph.write_png('tree3.png')
	prediction = clf.predict([[day,time,violation]])                                         
	return prediction


#@app.route('/charts')

#def render():
#	results=charts()
#	return render_template('charts.html',results=results)
'''
def charts():
    #full_filename = os.path.join(app.config['UPLOAD_FOLDER'], 'seaborn.png')
	
	raw_df=pd.DataFrame.from_csv("/home/student/Downloads/spark-1.6.0-bin-hadoop2.6/data.csv")
	print raw_df

	# Calculate #violations for each month

	Kot = len(raw_df.loc[raw_df['Violation Area'] == 'Kothrud'])
	Era = len(raw_df.loc[raw_df['Violation Area'] == 'Erandwane'])
	Vim = len(raw_df.loc[raw_df['Violation Area'] == 'VimanNagar'])
	Cam = len(raw_df.loc[raw_df['Violation Area'] == 'Camp'])
	Swa = len(raw_df.loc[raw_df['Violation Area'] == 'Swargate'])

	Total = len(raw_df)
	print Kot
	# Plot months vs violations percentage

	Area = ['Kothrud','Erandwane','VimanNagar','Camp','Swargate']
	Violations = [Kot*100/Total, Era*100/Total, Vim*100/Total, Cam*100/Total, Swa*100/Total]

	
	LABELS = ["Kothrud", "Erandwane", "Viman Nagar", "Camp", "Swargate"]
	plt.bar(Area, Violations, align='center', color = 'yellow')
	
	plt.xticks(Area, LABELS)

	plt.xlabel('Area')
	plt.ylabel("%Violations")
	plt.title('Area vs %Violations bar-chart')
	#plt.show()
	figfile = BytesIO()
    	plt.savefig(figfile, format='png')
    	figfile.seek(0)  # rewind to beginning of file
    	figdata_png = base64.b64encode(figfile.getvalue())
    	#figdata_png = urllib.quote(base64.b64encode(figfile.read()).decode())
    	return figdata_png
	

	# Create the Plotly Data Structure
    	graph = dict(
        	data=[go.Bar(
            		x=Area,
            		y=Violations
        	)],
        	layout=dict(
            		title='Bar Plot',
            		yaxis=dict(
                		title="%Violations"
            		),
            		xaxis=dict(
                	title="Areas"
            		)
        	)
    	)

    # Convert the figures to JSON
    	graphJSON = json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)
	return render_template('plotly.html',graphJSON=graphJSON)
	
'''
if __name__ == '__main__':
    app.secret_key = os.urandom(24)
    app.debug=True
    app.run()

