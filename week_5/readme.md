### Can't Connect to Jupyter Notebook
Ran into this strange issue where jupyter notebook webpage hosted in google ubuntu VM was not loading in local browser even after forwarding the port. Needed to create a tunnel using the following command to get arounf the isse.



### Module not found - Pyspark
The error message "ModuleNotFoundError: No module named 'pyspark'" indicates that the 'pyspark' module is not found in the Python environment used by Jupyter Notebook. This can happen if the necessary configurations for PySpark are not properly set up in Jupyter Notebook.
To resolve this issue, you can try the following steps:
1. Install the 'findspark' package by running the following command in Jupyter Notebook:
   
python
   !pip install findspark
   
2. Import and initialize 'findspark' at the beginning of your Jupyter Notebook script:
   
python
   import findspark
   findspark.init()
   
   This will add PySpark to the Python path in Jupyter Notebook.
3. Import 'pyspark' and create a Spark session as usual:
   
python
   import pyspark
   from pyspark.sql import SparkSession

   spark = SparkSession.builder \
       .appName('test') \
       .getOrCreate()
   
By following these steps, you should be able to import 'pyspark' and create a Spark session in Jupyter Notebook without encountering the 'ModuleNotFoundError' error.
Please note that if you have installed PySpark using 'pip3', you will need to run '!pip3 install pyspark' instead of '!pip install pyspark' in Jupyter

## Downgrade python

conda create -n sep_2021 python=3.9


# create the new environment without asking for confirmation
$ conda create --name play_environment python=3.9 pandas=1.5.3 jupyter ipykernel tabulate -y

# activate the environment
$ conda activate play_environment

# use ipykernel to register your new environment as a kernel named
# play environment
(play_environment) $ python -m ipykernel install --user --name play_environment --display-name "play environment"
