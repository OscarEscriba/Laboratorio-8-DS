# Importar la función 'col' que es necesaria para referirse a columnas
from pyspark.sql.functions import col 

# 1. Agrupar por 'año' y 'departamento' y contar los registros en cada grupo
accidentes_por_año_depto = df_hechos.groupBy("año", "departamento").count()

# 2. Opcional: Ordenar el resultado para una mejor visualización
accidentes_por_año_depto = accidentes_por_año_depto.orderBy("año", "departamento")

# 3. Usar la función display() para mostrar la tabla y el gráfico
display(accidentes_por_año_depto) 

# dia de la semana con más accidentes 
# Filtrar los datos para incluir únicamente el año 2023
df_2023 = df_hechos.filter(col("año") == 2023)
# Agrupar por día de la semana y contar los accidentes
accidentes_2023_por_dia = df_2023.groupBy("dia_semana").count()
# Ordenar los resultados de forma descendente para ver el día con más accidentes primero
accidentes_2023_por_dia = accidentes_2023_por_dia.orderBy(col("count").desc())
# Usar display() para mostrar la tabla y el gráfico de columnas (barras verticales)
display(accidentes_2023_por_dia)

#distribuución de accidentes por hora del día
df_municipio_gt = df_hechos.filter(col("municipio") == "GUATEMALA")
#Agrupar por la columna de la hora y contar los accidentes
accidentes_gt_por_hora = df_municipio_gt.groupBy("hora").count()
#Ordenar por hora para que el histograma tenga un orden lógico
accidentes_gt_por_hora = accidentes_gt_por_hora.orderBy("hora")
#Usar display() y seleccionar el tipo de gráfico 'Histograma'
display(accidentes_gt_por_hora)