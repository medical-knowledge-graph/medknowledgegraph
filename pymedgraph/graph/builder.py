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

    def build_graph(self, disease: str, paper: dict, entity_links: set):
        """
        Method to create new graph on neo4j instance.
        First the method deletes current nodes + relations on instance and creates single node with search term -> disease.
        Second, paper nodes + relations to disease are created.
        Then entities found in abstracts are created (nodes + relations to paper)
        Lastly, the links of entities to another knowledge base are created (nodes + relations to entities)

        :param disease: str - term of pubmed search
        :param paper: dict - contains pubmed fetch result + info extraction. looks similar to
        {k: {title: '', entities: [(e-text, e-label)]}} where k: paper id, title: paper title, entities: list of tuples
        with entity text and label.
        :param entity_links: set - contains tuples (e, c, n, d) -> e:entity, c:cui (concept id of link), n:name of
        concept, d:definition of concept.
        """
        self._init_new_neo4j_graph(disease)
        self.add_paper(paper)
        self.add_entities(paper)
        self.add_entity_links(entity_links)

    def add_entity_links(self, entity_links: set):
        """
        :param entity_links: set - contains tuples (e, c, n, d) -> e:entity, c:cui (concept id of link), n:name of
        concept, d:definition of concept.
        """
        df_ent_links = self._build_entity_links_df(entity_links)

        # add entity link nodes
        query = '''
                    UNWIND $rows AS row
                    CREATE (go:GO {cui: row.CUI, name: row.Name, definition: row.Definition})
                    RETURN count(*) as total
                '''
        self.insert_data(query, df_ent_links.drop('entity', axis=1).drop_duplicates())

        # add relations to entities
        query = '''
                    UNWIND $rows AS row
                    MATCH (e:Entity), (go:GO)
                    WHERE e.text = row.entity AND go.cui = row.CUI
                    CREATE (e)-[:CONTAINS]->(go)
                    RETURN count(*) as total
                '''
        resp = self.insert_data(query, df_ent_links[['entity', 'CUI']].drop_duplicates())
        print(resp)

    def add_entities(self, paper: dict, batch_size: int = 1000):
        entity_relations = self._build_entity_relations(paper)
        entity_nodes = self._build_entity_nodes(entity_relations)

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

        # add paper nodes
        query = '''
                    UNWIND $rows AS row
                    CREATE (p:Paper {uri: row.paper_uri, title: row.title})
                    RETURN count(*) as total
                '''

        resp = self.insert_data(query, df, batch_size)
        print(resp)
        # add relations to disease
        query = '''
                    MATCH (d:Disease), (p:Paper)
                    CREATE (d)-[:FOUND_IN]->(p)
                    RETURN count(*) as total
                '''
        resp = self.query(query, None)
        print(resp)

    @staticmethod
    def _build_entity_links_df(entity_links: set) -> pd.DataFrame:
        # build DataFrame from list of tuples
        ents = list()
        cuis = list()
        names = list()
        defs = list()
        for link in entity_links:
            ents.append(link[0])
            cuis.append(link[1])
            names.append(link[2])
            defs.append(link[3])
        return pd.DataFrame({'entity': ents, 'CUI': cuis, 'Name': names, 'Definition': defs})


    @staticmethod
    def _build_entity_relations(result_dict: dict) -> pd.DataFrame:
        ids = list()
        entities = list()
        for key, value in result_dict.items():
            ids.append(key)
            entities.append(value['entities'])
        df = pd.DataFrame({'uri': ids, 'entities': entities}).explode('entities').dropna()
        # split up tuple (entity-text, entity-label)
        df['entity_text'], df['entity_label'] = zip(*df['entities'])

        return df

    @staticmethod
    def _build_entity_nodes(df: pd.DataFrame) -> pd.DataFrame:
        # combine different entity labels into one list
        df = df.groupby('entity_text')['entity_label'].unique().apply(list).reset_index()
        return df

    def insert_data(self, query: str, rows: pd.DataFrame, batch_size: int = 2000):
        """ insert given query with rows batch wise into neo4j """
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

    def _init_new_neo4j_graph(self, disease: str):
        """ Method deletes graph and build new node for disease """
        delete_query = "MATCH (n) DETACH DELETE n"
        response = self.query(delete_query, None)
        print(response)
        init_query = "CREATE (d:Disease {label: $disease})"
        response = self.query(init_query, {'disease': disease})
        print(response)

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()


