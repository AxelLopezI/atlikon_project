# Datos de muestra

Archivos pequeños de muestra sintéticos que reflejan la estructura exacta de columnas que los notebooks de la capa *bronze* esperan recibir desde `s3://atlikon-dp/`. Su propósito es permitir la comprensión y ejecución de prueba del pipeline sin necesidad de acceder al bucket de S3 real ni a los datos originales.

Estos **no** son los datos reales del proyecto; los valores son ficticios.

| Archivo | Corresponde a la fuente | Columnas sin procesar (*raw*) |
|---|---|---|
| `customers.csv` | `s3://atlikon-dp/customers/*.csv` | `customer_id`, `customer_name`, `city` |
| `products.csv` | `s3://atlikon-dp/products/*.csv` | `product_id`, `product_name`, `category` |
| `gross_price.csv` | `s3://atlikon-dp/gross_price/*.csv` | `product_id`, `month`, `gross_price` |
| `orders.csv` | `s3://atlikon-dp/orders/landing/*.csv` | `order_id`, `order_placement_date`, `customer_id`, `product_id`, `order_qty` |

## Filas "sucias" intencionadamente

Cada archivo incluye algunas filas diseñadas específicamente para poner a prueba la lógica de calidad de datos implementada en la capa *silver*; de este modo, es posible observar cómo los pasos de limpieza realmente surten efecto al ejecutarse con esta muestra:

- **customers.csv** — nombres de ciudad mal escritos (`Bengaluruu`, `Hyderbad`), una ciudad escrita sin espacios (`NewDelhi`), espacios en blanco al inicio o al final de un nombre, y ciudades en blanco para los mismos `customer_id` que cuentan con correcciones codificadas explícitamente en `2_customers_silver_process.py` (`789403`, `789420`, `789521`, `789603`). 
- **products.csv** — una categoría o nombre mal escrito (`Protien Bar`) y un `product_id` no numérico (`ABC123`) para activar la lógica de respaldo al valor `999999`. 
- **gross_price.csv** — cuatro formatos de fecha distintos en la columna `month`, un precio negativo y un precio no numérico (`NA`). 
- **orders.csv** — una fecha que comienza con el nombre del día de la semana, tres formatos de fecha distintos, un `customer_id` no numérico (`ABCX`) y un valor `order_qty` ausente.

## Cómo utilizarlos localmente

Para ejecutar el pipeline con estos datos de muestra en lugar de usar S3, apunte la variable `base_path` de los notebooks de la capa *bronze* a una ubicación de volumen de Databricks o DBFS donde haya cargado estos archivos, manteniendo la misma estructura `<data_source>/*.csv` (y `orders/landing/*.csv` para los pedidos); por ejemplo:

```
/Volumes/fmcg/sample/atlikon-dp/
├── customers/customers.csv
├── products/products.csv
├── gross_price/gross_price.csv
└── orders/landing/orders.csv
```

Esto es opcional; su único propósito es permitir probar el funcionamiento del pipeline de principio a fin sin necesidad de utilizar sus credenciales de AWS. 