# ODT FLOW: Extracting, Querying, and Sharing Multi-source Multi-Scale Human Mobility

In response to the soaring needs of human mobility data, especially during disaster events such as the COVID-19 pandemic, and the associated big data challenges, we develop a scalable online platform for extracting, analyzing, and sharing multi-source multi-scale human mobility flows. Within the platform, an origin-destination-time (ODT) data model is designed to work with scalable query engines to handle heterogenous mobility data in large volumes with extensive spatial coverage, which allows for efficient extraction, query, and aggregation of billion-level origin-destination (OD) flows in parallel at the server-side. An interactive spatial web portal, ODT Flow Explorer, is developed to allow users to explore multi-source mobility datasets with user-defined spatiotemporal scales. To promote reproducibility and replicability, we further develop ODT Flow REST APIs that provide researchers with the flexibility to access the data programmatically via workflows, codes, and programs. Demonstrations are provided to illustrate the potential of the APIs when it is integrated with scientific workflows and with the Jupyter Notebook environment. We believe the platform can assist human mobility monitoring and analysis during disaster events such as the ongoing COVID-19 pandemic and benefit both scientific communities and the general public in understanding human mobility dynamics.

* Explore, visualize, and download the ODT flow data using the ODT Flow Explorer: http://gis.cas.sc.edu/GeoAnalytics/od.html
* Access the ODT flow data programmatically using the ODT Flow REST APIs with Jupyter Notebook: https://github.com/GIBDUSC/ODT_Flows/blob/main/ODT%20Flow%20REST%20APIs_Notebook_Tutorial.ipynb
* Case studies of accessing the ODT flow data in the KNIME workflow computing environment: https://github.com/GIBDUSC/ODT_Flow/tree/main/KNIME%20workflow%20case%20studies
* Learn how to use the Explorer with the Video Tutorial: https://www.youtube.com/watch?v=lV3AJIVYnSI
* Learn more about the system at: https://www.researchgate.net/publication/350342301_ODT_FLOW_A_Scalable_Platform_for_Extracting_Analyzing_and_Sharing_Multi-source_Multi-scale_Human_Mobility_Flows

All OD flows are downloaded in CSV (Comma-Separated Values) format. o_place refers to the id for the origin of the flow (using FIPS for places for US county and tract, using ISO place code for other places). d_place refers to the id for the destination of the flow (using FIPS for places for US county and tract, using ISO place code for other places).

Data updates: 

* Twitter-based flows are available from 01/01/2019 to 12/31/2020
* Safegraph-based flows are available from 01/01/2019 to 04/15/2021 (updated on April 18, 2021)

![alt text](http://gis.cas.sc.edu/gibd/wp-content/uploads/2020/11/ODT2.png)


