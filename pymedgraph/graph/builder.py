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

    @staticmethod
    def build_entity_relations(result_dict):
        ids = list()
        entities = list()
        for key, value in result_dict.items():
            ids.append(key)
            entities.append(value['entities'])
        df = pd.DataFrame({'uri': ids, 'entities': entities}).explode('entities')
        # split up tuple (entity-text, entity-label)
        df['entity_text'], df['entity_label'] = zip(*df['entities'])

        return df

    @staticmethod
    def build_entity_nodes(df):
        # combine different entity labels into one list
        df = df.groupby('entity_text')['entity_label'].unique().apply(list).reset_index()
        return df

    def add_entities(self, paper: dict, batch_size: int = 1000):
        entity_relations = self.build_entity_relations(paper)
        entity_nodes = self.build_entity_nodes(entity_relations)

        # add nodes
        query = '''
                    UNWIND $rows AS row
                    CREATE (e:Entity {text: row.entity_text, labels: row.entity_label})
                    RETURN count(*) as total
                '''
        self.insert_data(query, entity_nodes, batch_size)
        # add relations
        query = '''
                    UNWIND $rows AS row
                    MATCH (p:Paper), (e:Entity)
                    WHERE p.uri = row.uri AND e.text = row.entity_text
                    CREATE (p)-[:CONTAINS]->(e)
                    RETURN count(*) as total
                '''
        self.insert_data(
            query, entity_relations.drop_duplicates(['uri', 'entity_text']).drop('entities', axis=1), batch_size
        )

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


