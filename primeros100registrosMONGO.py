import pandas as pd
import pymongo
import json
#Me conecto al cliente del shell de MongoDb
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/shell")
#Importo el csv para almacenar los primeros 100 registros
libros = pd.read_csv("books.csv",nrows=100, delimiter=";", encoding='latin-1')
#me conecto a la base de datos shell, de esta manera puedo acceder a las colecciones que he creado
db = client["shell"]
#separo en dos dataframes los datos
libros_df = libros[['ISBN', 'Book-Title', 'Year-Of-Publication']]
libros_df.columns = ['isbn', 'Titulo', 'año_edicion']
autores_df = libros[['ISBN','Book-Author']]
#convierto uno de ellos a json, ya que puedo almacenarlo directamente
libros_json = libros_df.to_dict(orient='records')
#insertamos los datos a una de las colecciones
db.libros.insert_many(libros_json)
id = 1
autor_ids = []
isbns = []
nombres = []
#vamos a recorrer el otro dataframe que se ha obtenido anteriormente para crear dos dataframes que contengan la información deseada para las otras dos colecciones.
columnas_autor = ['autor_id','nombre_autor']
columnas_autor_isbn = ['autor_id', 'isbn']
df_autor = pd.DataFrame(columns=columnas_autor)
df_autor_isbn = pd.DataFrame(columns=columnas_autor_isbn)
for ind, fila in autores_df.iterrows():
  isbn = fila['ISBN']
  autor = fila['Book-Author']
  autor_id = id
  fila_autor = {'autor_id':autor_id, 'nombre_autor':autor}
  fila_autor_isbn = {'isbn':isbn, 'autor_id':autor_id}
  df_autor = pd.concat([df_autor,pd.DataFrame([fila_autor])], ignore_index=True)
  df_autor_isbn = pd.concat([df_autor_isbn,pd.DataFrame([fila_autor_isbn])], ignore_index=True)
  id += 1
#Una vez tenemos toda la información que necesitamos en los dos dataframes, les cambiamos el formato a un diccionario y los insertamos en sus respectivas colecciones.
autores_json = df_autor.to_dict(orient='records')
autores_isbn_json = df_autor_isbn.to_dict(orient='records')

db.autores.insert_many(autores_json)
db.autor_isbn.insert_many(autores_isbn_json)
#Por último, salimos del cliente.
client.close()

