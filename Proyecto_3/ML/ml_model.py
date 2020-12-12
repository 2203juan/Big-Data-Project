from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import IntegerType,DoubleType
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.ml.regression import RandomForestRegressor




# Se muestra el esquema del dataset
spark = SparkSession.builder.appName('ml-eshop').getOrCreate()
df = spark.read.format("csv").option("delimiter", ",").option("header",True).load("normal.csv")
df.printSchema()


# castear todos los numeros cargados como string a int
for col in df.columns:
    df = df.withColumn(col,df[col].cast(IntegerType()))


# Se agrupa el dataset de acuerdo a la variable objetivo
labeled = df.groupby("clicks").count()
#labeled.show()


# Se guarda en la lista names los nombres de las columnas exceptuando la variable objetivo
names = list()
for i in df.columns:
    if i!="clicks":
        names.append(i)

# Se convierte a vectorAssembler para poder usar los modelos de Machine Learning

vectorAssembler = VectorAssembler(inputCols = names, outputCol = 'features')
vhouse_df = vectorAssembler.transform(df)
vhouse_df = vhouse_df.select(['features', 'clicks'])

# Se parte el conjunto de datos en entrenamiento  = 70% y prueba = 30%
splits = vhouse_df.randomSplit([0.7, 0.3])



# se guarda cada subconjunto en variables diferentes
train_df = splits[0] 
test_df = splits[1] 
#train_df.show()
#test_df.show()


# Modelos de Regresionn
# Random Forest Regressor

# se crea y entrena el modelo
rf = RandomForestRegressor(labelCol="clicks", maxBins=217, seed = 2)
model = rf.fit(train_df)

rf_prediction = model.transform(test_df)
test_prediction_rf = rf_prediction.select("prediction","clicks")

evaluator = RegressionEvaluator(labelCol="clicks")

print("\nModelo Random Forest Regressor")
print("R Squared (R2) on test data = %g" % evaluator.evaluate(test_prediction_rf, {evaluator.metricName: "r2"}))
print("Root Mean Squared Error (RMSE) on test data = %g" % evaluator.evaluate(test_prediction_rf, {evaluator.metricName: "rmse"}))
print(model.featureImportances)
