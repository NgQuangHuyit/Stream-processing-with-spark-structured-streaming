class Log4j:
    def __init__(self, spark):
        conf = spark.sparkContext.getConf()
        self.log4j = spark._jvm.org.apache.log4j
        app_name = conf.get("spark.app.name")
        root_class = "com.nqh.sparkstreaming.examples"
        self.logger = self.log4j.LogManager.getLogger(root_class + "." + app_name)

    def warn(self, msg):
        self.logger.warn(msg)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)

    def debug(self, msg):
        self.logger.debug(msg)