# TrafficDataAnalysis
According to the World Health Organization, India fares pooly in the area of road traffic fatalities. In a report published by the Indian Ministry of Road and Transport Highway, the biggest reason for road-related fatalities accidents is road-speeding with 70% of all accidents and 67% of traffic deaths in 2017 attributed to drivers breaching speed limits. Therefore, in my opinion, reducing traffic violations could have a significant impact on making Indian roads safer.

In this project vast amounts of data collected by the E-challan(online traffic violation) system of two major Indian cities, Pune and Mumbai, is analysed. By cleaning, preprocessing and applying appropriate machine learning techniques to the data, we could answer questions like "On a weekday during rush hours which area of the city is most prone to traffic violations?", "In Area X of City Y, what is the most common traffic violation?", "Can Areas be clustered based on the traffic violations?". Given that traffic monitoring is largely unatomated in India, this analysis, attemps to help the traﬃc police to strategically monitor the traﬃc and accordingly take actions for the safety and beneﬁt of citizens.

Another requirement of the company that manages the E-Challan system and provided us with the real data is that they wanted a robust system that could keep up with the rapid scaling of the traffic management system. Hence, the first part of the project proposes the migration of the existing data on postgres to Hadoop ecosystem using simple scripts. Then all the data on Hadoop is analysed using PySpark. Finally to build a end-end product, we built a dashboard using Flask and Plotly to see results of the anaylsis.

Some of the results of the data analysis look as follows:

![Alt text](/static/dp1.png?raw=true "Title")
