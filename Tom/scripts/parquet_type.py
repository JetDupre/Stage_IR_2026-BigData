import pyarrow.parquet as pq 
schema = pq.read_schema(r'./sources/food.parquet') 
print(schema.field('product_name'))