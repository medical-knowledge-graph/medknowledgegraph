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

    def build_biomed_graph(self, disease: str, pipe_outputs):
        """
        :param disease: str - name of search term
        :param pipe_outputs: list - contains pymedgraph.dataextraction.basepipe.PipeOutput objects
        """
        self._init_new_neo4j_graph(disease)
        for output in pipe_outputs:
            for node_table in output.node_tables:
                self.upload_nodetable(node_table)

    def upload_nodetable(self, node_table):
        # determine if node table contains multiple node labels
        if isinstance(node_table.meta['node_label'], list):
            for node_label in node_table.meta['node_label']:
                # get query for table
                query = self._create_node_query(node_table.meta, node_label)
                # do upload
                self.insert_data(
                    query,
                    self.filter_node_data(node_table, filter_label=node_label, drop_duplicates=True)
                )
                # TODO: ugly -.-
                if isinstance(node_table.meta['source_node'], list):

                    for source_node in node_table.meta['source_node']:
                        # get query for relations
                        query = self._create_node_relation_query(node_table.meta, node_label, source_node)
                        # do upload
                        self.insert_data(
                            query,
                            self.filter_node_data(node_table, filter_label=node_label, drop_duplicates=False)
                        )
                else:
                    query = self._create_node_relation_query(
                        node_table.meta, node_label, node_table.meta['source_node']
                    )
                    # do upload
                    self.insert_data(
                        query,
                        self.filter_node_data(node_table, filter_label=node_label, drop_duplicates=False)
                    )
        else:
            # get query for table
            query = self._create_node_query(node_table.meta, node_table.meta['node_label'])
            # do upload
            self.insert_data(query, self.filter_node_data(node_table, drop_duplicates=True))
            if isinstance(node_table.meta['source_node'], list):
                print('correct')
                for source_node in node_table.meta['source_node']:
                    # get query for relations
                    query = self._create_node_relation_query(node_table.meta, node_table.meta['node_label'], source_node)
                    # do upload
                    self.insert_data(query, self.filter_node_data(node_table))
            else:
                print('wrong')
                query = self._create_node_relation_query(
                    node_table.meta, node_table.meta['node_label'], node_table.meta['source_node']
                )
                # do upload
                self.insert_data(query, node_table.data)

    @staticmethod
    def filter_node_data(node_table, filter_label=None, drop_duplicates=False) -> pd.DataFrame:
        if filter_label:
            df = node_table.data[node_table.data['node_label'] == filter_label]
        else:
            df = node_table.data
        if drop_duplicates:
            df = df.drop_duplicates(subset=[node_table.meta['id_attribute']])
        return df

    @staticmethod
    def get_node_data(node_table):
        return node_table.data.drop_duplicates(subset=[node_table.meta['id_attribute']])

    @staticmethod
    def _create_node_query(node_table_meta: dict, node_label: str):
        """ Build query to upload nodes with attributes """
        # collect columns
        attribute_cols = [node_table_meta['id_attribute']]
        if node_table_meta['attribute_cols']:
            attribute_cols += node_table_meta['attribute_cols']
        # build string for node attributes
        node_attr_string = ''
        for i, col in enumerate(attribute_cols):
            node_attr_string += f'{col}: row.{col}'
            if i + 1 != len(attribute_cols):
                node_attr_string += ', '
        # build node string
        node_string = 'CREATE (n: {n} {{{attr}}})'.format(n=node_label, attr=node_attr_string)
        # build final query
        query = 'UNWIND $rows AS row ' + node_string + ' RETURN count(*) as total'
        return query

    @staticmethod
    def _create_node_relation_query(node_table_meta: dict, node_label: str, source_node_label: str):
        """ Build query to upload node relations to source nodes """
        match_query = 'MATCH (a: {A}), (b: {B})'.format(A=source_node_label, B=node_label)
        where_query = 'WHERE a.{a_attr} = row.{a_col} AND b.{b_attr_col} = row.{b_attr_col}'.format(
            a_attr=node_table_meta['source_node_attr'], a_col=node_table_meta['source_column'],
            b_attr_col=node_table_meta['id_attribute']
        )
        query = 'UNWIND $rows AS row ' + match_query + ' ' + where_query + \
                ' CREATE (a)-[:CONTAINS]->(b) RETURN count(*) as total'
        return query

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
        init_query = "CREATE (st:SearchTerm {label: $disease})"
        response = self.query(init_query, {'disease': disease})
        print(response)

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()


