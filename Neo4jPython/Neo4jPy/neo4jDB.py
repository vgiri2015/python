__author__ = 'gfp2ram'
import neo4j
connection = neo4j.connect("http://localhost:7474")
cursor = connection.cursor()
cursor.execute("CREATE (n:User {name:'Stevie Brook'}")