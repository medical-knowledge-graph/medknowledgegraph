import time
import pandas as pd
from neo4j import GraphDatabase


class Neo4jBuilder(object):
    """
    Using batch upload instead of session + for loop
    https://towardsdatascience.com/create-a-graph-database-in-neo4j-using-python-4172d40f89c4
    """

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def add_paper(self, paper: dict, batch_size: int = 1000):
        paper_uris = list()
        titles = list()
        for paper_id, paper_val in paper.items():
            paper_uris.append(paper_id)
            titles.append(paper_val['title'])
        df = pd.DataFrame({'paper_uri': paper_uris, 'title': titles})
        query = '''
                    UNWIND $rows AS row
                    CREATE (p:Paper {uri: row.paper_uri, title: row.title})
                    RETURN count(*) as total
                '''
        return self.insert_data(query, df, batch_size)

    def insert_data(self, query, rows, batch_size=2000):
        total = 0
        batch = 0
        start = time.time()
        result = None

        while batch * batch_size < len(rows):
            res = self.query(
                query,
                parameters={'rows': rows[batch * batch_size:(batch + 1) * batch_size].to_dict('records')})
            total += res[0]['total']
            batch += 1
            result = {"total": total,
                      "batches": batch,
                      "time": time.time() - start}
            print(result)

        return result
    def query(self, query, parameters):
        """make actual query"""
        session = None
        response = None
        try:
            session = self.driver.session()
            response = list(session.run(query, parameters))
        except Exception as ex:
            print('Query failed:', ex)
        finally:
            if session:
                session.close()
        return response

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()


