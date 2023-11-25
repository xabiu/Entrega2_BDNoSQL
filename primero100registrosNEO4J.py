from neo4j import GraphDatabase
import pandas as pd

#Guardamos en varias instancias la información necesaria para conectarse al cliente de Neo4j.
uri = "neo4j://localhost:7687"
username = "neo4j"
password = "master22"
#Esta función va a recibir todos los datos que se necesitan para crear los nodos y para crear la relación entre los dos.
def insertar_datos(isbn, autor, titulo, año, id_autor, identAutor, identLibro):
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        with driver.session() as session:
            # Crea un nodo Libros y otro Autores. Una vez creados, crea una relación entre estos dos nodos.
            session.run("CREATE(:Libros {isbn: $isbn, titulo: $titulo, año_edicion: $año})", isbn=isbn, titulo=titulo, año=año)
            session.run("CREATE(:Autores {id_autor: $id_autor, nombre_autor: $autor})", id_autor=id_autor, autor=autor)
            session.run("MATCH (a:Autores), (b:Libros) WHERE a.id_autor = $id_autor AND b.isbn = $isbn CREATE (a)-[:ESCRIBIO]->(b)", id_autor=id_autor, isbn=isbn)
#Importamos el dataset y recorremos los 100 primeros registros almacenando los datos por medio de la función "insertar_datos"
libros = pd.read_csv("books.csv", nrows=100, delimiter=";", encoding='latin-1')
id = 2
for ind, fila in libros.iterrows():
    isbn = fila['ISBN']
    autor = fila['Book-Author']
    titulo = fila['Book-Title']
    año = fila['Year-Of-Publication']
    id_autor = id
    identAutor = "autor" + str(id)
    identLibro = "libro" + str(id)
    insertar_datos(isbn, autor, titulo, año, id_autor, identAutor, identLibro)
    id += 1

