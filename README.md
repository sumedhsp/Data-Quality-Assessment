# Data Quality Assessment
An open source PySpark framework for assessing the Data Quality of a given dataset.

The DQA.py file contains the master data code which assesses the provided dataset on the following metrics:
1. Uniqueness
2. Completeness
3. Range Adherence
4. Format Adherence
5. Master Data Adherence
6. Data Duplication
7. Outlier Detection

a) The program requires user input in the form of the address of the dataset and the YAML configuration file.

b) For functions masterDataAdherence as well as getOutlierScores, the user needs to specify the target columns as well.

c) The program returns all the seven scores of the data dimensions of the respective dataset.
