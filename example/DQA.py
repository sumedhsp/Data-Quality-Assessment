class DataQualityIndexOf:
    # List of dimensions
    uniqueness_ = "Uniqueness"
    completeness_ = "Completeness"
    rangeAdherence_ = "RangeAdherence"
    formatAdherence_ = "FormatAdherence"
      
    def __init__(self, path_to_csv=None, yaml_file=None):
        
        # Validating the path_to_csv variable
        if path_to_csv == None:
            raise "Path to csv not provided"
        else:
            self.table_df = spark.read.csv(path=path_to_csv,header=True)
        
        # Validating the yaml_file variable
        if (yaml_file == None):
            print ("Warning: No yaml file found, so going ahead with calculations of only intrinsic scores!")
        else:
            self.yaml_read = yaml.safe_load(open(yaml_file,'r'))

        hashlib.sha1().update(str(time.time()).encode("utf-8"))

        # Creating a temporary table name for spark sql queries and further operations
        self.table_name = "table_"+hashlib.sha1().hexdigest()[:5]
        self.table_df.createOrReplaceTempView(self.table_name)

        # List of dimensions
        self.Dimensions = [self.uniqueness_,self.completeness_,self.rangeAdherence_,self.formatAdherence_]

        # Uniqueness constraints details
        self.uniq = None
        self.unique_c = {}

        # Completeness constraints details
        self.complete = None
        self.complete_c = {}

        # Range Adherence details
        self.rangeadherence = None
        # can store the column name and the score for each column
        # that way whenever the user wants to know the score for a particular column,
        # we can display it.
        self.rangeadherence_c = {}

        # Format Adherence details
        self.formatadherence = None
        self.formatadherence_c = {}

        # Total records
        self.totalRows = self.table_df.count()

      # Do we really need this method?
    def loadFromCsv(self, path_to_csv):
        self.table_df = spark.read.csv(path=path_to_csv,header=True)

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
        for col in self.table_df.columns:
            uniq_i = self.table_df.select(col).distinct().count() / self.totalRows
            unique_list.append(uniq_i)

            # Getting all the highest uniqueness scores
            # and creating a dictionary record for those columns.
            if uniq_i==1:
                self.uniq=1
                self.unique_c[col] = 1

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
        user_defined_ips = self.yaml_read[self.completeness_]

        self.complete_c = []

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

        # Adhering to the formats provided by the user through the YAML file
        user_ip_dict = self.yaml_read[self.formatAdherence_]
        for column in user_ip_dict.keys():
            format_pattern = user_ip_dict[column]

            # converting the dataframe into rdd to apply transformation
            temp_rdd = self.table_df.select(column).rdd.flatMap(list)

            # Getting the rows which match the format provided in the YAML file for the column.
            fmt_adher = temp_rdd.map(lambda x: matchPattern(x,format_pattern)).filter(lambda x: x != None).count()

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
