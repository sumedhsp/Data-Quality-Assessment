from masterdata.MasterDataS3 import MasterDataS3 as msd3
import hashlib
import time
from pyspark.sql.functions import sha2, concat_ws, approx_count_distinct, col

class DataQualityIndexOf:
    # List of dimensions
    uniqueness_ = "Uniqueness"
    completeness_ = "Completeness"
    rangeAdherence_ = "RangeAdherence"
    formatAdherence_ = "FormatAdherence"
    masterDataAdherence_ = "MasterDataAdherence"
    dataDuplication = "DataDuplication"
    outlierDetection = "OutlierDetection"

    def __init__(self, spark_session=None ,path_to_csv=None, yaml_file=None):
        
        self.spark = spark_session
        # Validating the path_to_csv variable
        if path_to_csv == None:
            raise "Path to csv not provided"
        else:
            self.table_df = self.spark.read.csv(path=path_to_csv,header=True)
        
        # Validating the yaml_file variable
        if (yaml_file == None):
            print ("Warning: No yaml file found, so going ahead with calculations of only intrinsic scores!")
        else:
            self.yaml_read = yaml.safe_load(open(yaml_file,'r'))

        hashlib.sha1().update(str(time.time()).encode("utf-8"))

        # Creating a temporary table name for spark sql queries and further operations
        self.table_name = "table_" + hashlib.sha1().hexdigest()[:5]
        self.table_df.createOrReplaceTempView(self.table_name)

        # List of dimensions
        self.Dimensions = [self.uniqueness_,self.completeness_,self.rangeAdherence_,self.formatAdherence_]

        # Storing the column names along with individual column scores.
        # Uniqueness constraints details
        self.uniq = None
        self.unique_c = {}

        # Completeness constraints details
        self.complete = None
        self.complete_c = {}

        # Range Adherence details
        self.rangeadherence = None
        self.rangeadherence_c = {}

        # Format Adherence details
        self.formatadherence = None
        self.formatadherence_c = {}
        
        # Master Data Adherence details
        self.masterData = None
        self.masterData_c = {}

        # Data duplicate info
        self.dataDuplication = None

        # Outlier Detection
        self.outlierScore = None
        self.outlierDict = {}

        # Total records
        self.totalRows = self.table_df.count()

    def loadFromCsv(self, path_to_csv):
        self.table_df = self.spark.read.csv(path=path_to_csv,header=True)

        hashlib.sha1().update(str(time.time()).encode("utf-8"))
        self.table_name = "table_"+hashlib.sha1().hexdigest()[:5]

        self.table_df.createOrReplaceTempView(self.table_name)

    # *** Method uniqueness begins here ***
    def uniqueness(self):
        """ To calculate the uniqueness constraint dimension in the dataset """

        # If the uniq value is already initialized in class instance
        # return the property value instead of computing in spark again
        if (self.uniq != None):
            return self.uniq

        # Initializing the default value of "self.uniq" property to 0
        self.uniq = 0

        unique_list = []
        for col_name in self.table_df.columns:
            uniq_i = self.table_df.select(col_name).distinct().count() / self.totalRows
            unique_list.append(uniq_i)

            # Getting all the highest uniqueness scores
            # and creating a dictionary record for those columns.
            if uniq_i==1:
                self.uniq=1
                self.unique_c[col_name] = 1

            self.uniq=max(self.uniq, uniq_i)

        return self.uniq

    # *** Method uniqueness ends here ***

    # *** Method completeness begins here ***
    def completeness(self):
        """ To calculate the completeness constraint dimension in the dataset """

        # If the complete dimension value is already calculated, we just return the score
        if (self.complete != None):
            return self.complete

        total_complete = 0
        complete_c = 0

        # Read the extrinsic details provided by the user for
        # the Completeness dimension
        if (self.completeness_ not in self.yaml_read.keys()):
            # As the method should calculate intrinsic data dimension
            user_defined_ips = {}
        else:
            user_defined_ips = self.yaml_read[self.completeness_]

        for c in self.table_df.columns:
            df2 = self.table_df.filter(col(c).isNotNull())

            # Including the user inputs from the YAML file for 
            # specific columns
            if (c in user_defined_ips.keys()):
                # Iterating through the list of invalid rules provided
                # by the user and filtering out the records accordingly
                for item in user_defined_ips[c]:
                    df2 = df2.filter(col(c) != item)

            complete_c = df2.count() / self.totalRows

            # Storing the column name and the respective completeness score
            self.complete_c[c] = complete_c

        total_complete += complete_c

        self.complete = total_complete / len(self.table_df.columns)

        return self.complete

    # *** Method completeness ends here ***

    # *** Method rangeAdherence begins here ***
    def rangeAdherence(self):
        """ To calculate the score for Range Adherence dimension in the dataset """

        # If the Range Adherence dimension value is already calculated, we just return the score
        if (self.rangeadherence != None):
            return self.rangeadherence

        # *** Inner method calculateRangeAdherenceScore begins here ***
        def calculateRangeAdherenceScore(df, col_name, user_input_range):
            """ To calculate the range adherence score based on the user input range for the column """

            # Validating the input file before further processing
            if (len(user_input_range) != 2):
                raise "Invalid input provided for Range Adherence -> " + str(user_input_range)

            if (user_input_range[0] == '-inf' and user_input_range[1] == 'inf'):
                # do nothing
                return df.count()
            elif (user_input_range[0] == '-inf'):
                # only upper condition needs to be applied
                return df.select(col_name).filter(df[col_name] <= user_input_range[1]).count()
            elif (user_input_range[1] == 'inf'):
                # only lower condition needs to be applied
                return df.select(col_name).filter(df[col_name] >= user_input_range[0]).count()
            else:
                # both conditions need to be applied
                return df.select(col_name).filter(df[col_name] >= user_input_range[0]).filter(df[col_name] <= user_input_range[1]).count()    

        # *** Inner method calculateRangeAdherenceScore ends here ***  

        total_complete = 0
        rng_adherence_c = 0

        # If the dimension is not defined in YAML, simply return no score 
        # Can be improved in the future to return None (instead of 1)
        if (self.rangeAdherence_ not in self.yaml_read.keys()):
            return 1
        else:
            # Adhering to the ranges provided by the user through the YAML file
            user_ip_dict = self.yaml_read[self.rangeAdherence_]
            
        for column in user_ip_dict.keys():
            rng_adher = calculateRangeAdherenceScore(self.table_df, column, user_ip_dict[column])

            rng_adherence_c = rng_adher / self.totalRows
            self.rangeadherence_c[column] = rng_adherence_c

            total_complete += rng_adherence_c

        # Calculating the average score based on the columns provided by the user
        self.rangeadherence = total_complete / len(user_ip_dict)
        return self.rangeadherence

    # *** Method rangeAdherence ends here ***

    # *** Method formatAdherence begins here ***
    def formatAdherence(self):
        """ To calculate the score for Format Adherence dimension in the dataset """

        # If the Format Adherence dimension value is already calculated, we just return the score
        if (self.formatadherence!= None):
            return self.formatadherence

        import re

        # *** Inner function matchPattern begins here ***
        def matchPattern(value, pattern):
            """ To check if the pattern exists in the string value provided """
            if (re.search(pattern, value)):
                return value
        # *** Inner function matchPattern ends here ***

        total_complete = 0
        frmt_adherence_c = 0
        
        # If the dimension is not defined in YAML, simply return no score 
        if (self.formatAdherence_ not in self.yaml_read.keys()):
            return 1
        else:
            # Adhering to the formats provided by the user through the YAML file
            user_ip_dict = self.yaml_read[self.formatAdherence_]
        
        for column in user_ip_dict.keys():
            format_pattern = user_ip_dict[column]

            # converting the dataframe into rdd to apply transformation
            temp_rdd = self.table_df.select(column).rdd.flatMap(list)

            # Getting the rows which match the format provided in the YAML file for the column.
            fmt_adher = temp_rdd.map(lambda x: matchPattern(str(x),format_pattern)).filter(lambda x: x != None).count()

            frmt_adherence_c = fmt_adher / self.totalRows
            self.formatadherence_c[column] = frmt_adherence_c

            total_complete += frmt_adherence_c

        self.formatadherence = total_complete / len(user_ip_dict)

        return self.formatadherence

    # *** Method formatAdherence ends here ***

    # *** Method getDQA begins here ***
    def getDQA(self):
        """ To get the aggregate data quality score for any given entire dataset """
        # Uniqueness dimension
        uniq = self.uniqueness()
        # Completeness dimension
        comp = self.completeness()
        # Range Adherence dimension
        rng = self.rangeAdherence()
        # Format Adherence dimension
        fmt = self.formatAdherence()

        return (uniq+comp+rng+fmt) / len(self.Dimensions)

    # *** Method getDQA ends here ***

    # *** Method getDimensionDetails begins here ***
    def getDimensionDetails(self, dimension):
        """ To display the data quality score for given dimension and all the columns calculated """

          # We can probably do better here!!
        if (dimension == self.Dimensions[0]):
            # Uniqueness constraint
            return self.unique_c
        elif (dimension == self.Dimensions[1]):
            # Completeness constraint
            return self.complete_c
        elif (dimension == self.Dimensions[2]):
            # Range Adherence constraint
            return self.rangeadherence_c
        elif (dimension == self.Dimensions[3]):
            # Format Adherence constraint
            return self.formatadherence_c

    # *** Method getDimensionDetails ends here ***
    
    def  masterDataAdherence(self, masterDataFile, targetColumns):
        """ To validate the data adherence with master dataset """
        
        # masterdatafile object
        mdfObj = msd3()
        masterDataSet = set()
        # get the masterdata data
        masterData_csv = mdfObj.retrieve_master_data_file(masterDataFile)
        # load it into a fast searchable data type like the set
        for row in masterData_csv:
            masterDataSet.update(row)
        
        total_masterDataCount = 0
        
        for targetColumn in targetColumns:
            data_count = self.table_df.select(targetColumn).rdd.flatMap(list).filter(lambda x: x in masterDataSet).collect()
            mda_c = len(data_count) / self.totalRows

            # Storing the column name and the respective completeness score
            self.masterData_c[targetColumn] = mda_c
            total_masterDataCount += mda_c

        self.masterData = total_masterDataCount / len(targetColumns)
        
        return self.masterData
        
    def dataDuplication(self, colsArray, approx=False):
        """ To check for data duplication in the dataset """
        
        # Concatenate specified columns
        df2 = self.table_df.withColumn("concatenated_column", concat_ws("", *[col(c) for c in colsArray]))
        
        # Encrypt the concatenated column using SHA-256
        df2 = df2.withColumn("hashed_column", sha2("concatenated_column", 256))

        if approx:
            y = df2.agg(approx_count_distinct("hashed_column")).collect()
            approx_count = y[0][0]
        else:
            approx_count = df2.select("hashed_column").distinct().count()
        
        self.dataDuplication = approx_count / self.totalRows
        return self.dataDuplication
    
    def getOutlierScores(self, columns_of_interest):
        """
        Detect outliers using the Interquartile Range (IQR) method for specified columns.

        Parameters:
        - columns_of_interest: List of column names for which outliers should be detected

        Returns:
        - Dictionary with column names as keys and the count of unique rows with outliers as values
        """
        result_dict = {}
        total_outliers_count = 0
        outlier_score = 0
        self.outlierScore = 0

        for col_name in columns_of_interest:
            # Calculate IQR for the current column
            quantiles = self.table_df.approxQuantile(col_name, [0.25, 0.75], 0.05)
            q1 = quantiles[0]
            q3 = quantiles[1]
            iqr = q3 - q1

            # Define lower and upper bounds for outliers
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr

            # Filter rows outside the bounds
            outliers_df = df.filter((col(col_name) < lower_bound) | (col(col_name) > upper_bound))

            outliers_count = outliers_df.count()
            unique_rows_with_outliers_count = df.select(columns_of_interest).distinct().count()
            if unique_rows_with_outliers_count != 0:
                outlier_score = 1 - (outliers_count/self.totalRows)
            # Add the result to the dictionary
            result_dict[col_name] = outlier_score
            self.outlierScore += outlier_score

        self.outlierScore /= len(columns_of_interest)
        self.outlierDict = result_dict
        
        return self.outlierScore
