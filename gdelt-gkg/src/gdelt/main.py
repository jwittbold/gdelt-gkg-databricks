
# https://menziess.github.io/howto/enhance/your-databricks-workflow/
# https://kedro.readthedocs.io/en/stable/10_deployment/08_databricks.html

# http://www.legendu.net/misc/blog/packaging-python-dependencies-for-pyspark-using-pex/
# https://www.pantsbuild.org/docs/python-package-goal
# https://pex.readthedocs.io/en/v2.1.65/api/vars.html
# https://kedro.readthedocs.io/en/stable/10_deployment/08_databricks.html


# https://medium.com/quantumblack/deploying-and-versioning-data-pipelines-at-scale-942b1d81b5f5
# https://code.visualstudio.com/docs/python/environments#_use-of-the-pythonpath-variable
# https://spark.apache.org/docs/latest/api/python/user_guide/python_packaging.html
# https://github.com/criteo/cluster-pack/blob/master/examples/spark-with-S3/README.md
# https://medium.com/criteo-engineering/packaging-code-with-pex-a-pyspark-example-9057f9f144f3
# https://sinoroc.gitlab.io/kb/python/pex.html

# /Users/jackwittbold/.pex

if __name__ == "__main__":

    import glob
    import os
    import sys
    import subprocess
    from pyspark.sql import SparkSession

    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    # import scrapers.gkg_scraper as scraper
    # import etl.execute_etl as etl
    from config.toml_config import config
    from scrapers.gkg_scraper import Execute_GKG_Scaper


    os.environ['PYSPARK_PYTHON'] = './gdelt_env'

    # pex . -r requirements.txt  -o ./dist/gdelt_pipeline_env.pex -f ./dist/gdelt_pipeline_jwittbold-1.0.0-py3-none-any.whl

    gdelt_pex_env = f"{os.environ['GDELT_HOME']}/gdelt-gkg/dist/gdelt_env.pex"
   

    def build_spark():

        spark = SparkSession.builder \
            .master('yarn') \
            .appName('gdelt-pipeline') \
            .config('spark.submit.deployMode', 'cluster') \
            .config("spark.yarn.dist.files", gdelt_pex_env) \
            .config('spark.executorEnv.PEX_ROOT', '~/.pex') \
            .getOrCreate()

        return spark

    spark = build_spark()


    try:
        scraper.Execute_GKG_Scaper()
    except Exception as e:
        print(f"Encountered exception when calling 'Execute_GKG_Scaper()' within main.py:\n{e}")
    try:
        etl.Execute_ETL()
    except Exception as e:
        print(f"Encountered exception when calling 'Execute_ETL()' within main.py:\n{e}")
