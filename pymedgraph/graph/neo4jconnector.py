import time
import pandas as pd
from neo4j import GraphDatabase


class Neo4jConnector(object):
    """
    Class is used with the method `Neo4jBuilder.build_biomed_graph` to upload a new subgraph to the initiated neo4j
    instance. It requires the pymedgraph.dataextraction.basepipe.PipeOutput objects, to extract all necessary information
    for the upload from the data tables.

    We use a batch upload with the "UNWIND" functionality instead of a session and for loops.

    Source for batch upload: https://towardsdatascience.com/create-a-graph-database-in-neo4j-using-python-4172d40f89c4
    """

    def __init__(self, uri, user, password, logger=None):
        """ Initializes Neo4jBuilder and constructs Neo4J driver based on credentials.

        :param uri: str - neo4j connection url
        :param user: str - username to connect to neo4j instance
        :param password: str - user password to connect to neo4j instance
        :param logger: logging.logger
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.logger = logger

    def build_biomed_graph(self, disease: str, pipe_outputs, delete_graph: bool = False):
        """
        Wrapper method to build new graph for disease and pipe output tables.

        :param disease: str - name of search term
        :param pipe_outputs: list - contains pymedgraph.dataextraction.basepipe.PipeOutput objects
        :param delete_graph: bool - flag if existing graph shall be deleted or not
        """
        self._init_new_neo4j_graph(disease, delete_graph)
        for output in pipe_outputs:
            if self.logger:
                self.logger.info('*** Start processing output for pipe \'{p}\'. ***'.format(p=output.pipe))
            for node_table in output.node_tables:
                try:
                    self.upload_nodetable(node_table)
                    if self.logger:
                        self.logger.info('Successfully uploaded node table \'{nt}\'.'.format(nt=node_table.name))
                except Exception as ex:
                    if self.logger:
                        self.logger.error('Failed upload for node table \'{n}\' with {ex}'.format(
                            n=node_table.name, ex=ex))
                    raise RuntimeError(ex)

    def upload_nodetable(self, node_table):
        """
        Method uploads the nodetable into a neo4j instance.

        Although the method handles different types of node tables and passes different parameters to sub functions,
        the pattern for the data upload is always the same:

        Nodes
        1. Building a query string for the nodes with `Neo4jBuilder._create_node_query`
        2. Passing the query to upload data `Neo4jBuilder.insert_data`
        Node Relations
        3. Building a query string for the node relations with `Neo4jBuilder._create_node_relation_query`
        4. Passing the query to upload data `Neo4jBuilder.insert_data`

        :param node_table: pymedgraph.dataextraction.basepipe.NodeTable
        """
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
                for source_node in node_table.meta['source_node']:
                    # get query for relations
                    query = self._create_node_relation_query(node_table.meta, node_table.meta['node_label'], source_node)
                    # do upload
                    self.insert_data(query, self.filter_node_data(node_table))
            else:
                query = self._create_node_relation_query(
                    node_table.meta, node_table.meta['node_label'], node_table.meta['source_node']
                )
                # do upload
                self.insert_data(query, node_table.data)

    @staticmethod
    def filter_node_data(node_table, filter_label=None, drop_duplicates=False) -> pd.DataFrame:
        """
        Method is used to either filter the node table for a given `node_label`, for instance "Gene" or
        to drop duplicates in table by a given attribute subset, or both.

        :param node_table: pymedgraph.dataextraction.basepipe.NodeTable
        :param filter_label: str - node lable, by which the data is selected
        :param drop_duplicates: bool
        """
        if filter_label:
            df = node_table.data[node_table.data['node_label'] == filter_label]
        else:
            df = node_table.data
        if drop_duplicates:
            df = df.drop_duplicates(subset=[node_table.meta['id_attribute']])
        return df

    @staticmethod
    def _create_node_query(node_table_meta: dict, node_label: str) -> str:
        """
        Build query to upload nodes with attributes.
        Method prepares query, which will be complemented with data via a pd.DataFrame. The query defines variables
        for the node label and it`s attributes. The variables will be replaced by the given data.

        To do a batch upload the query contains the `UNWIND` statement.

        *** NOTE ***
            We us `MERGE` instead of `CREATE` in the Cypher query to avoid duplicates!
            This way the node is only going to be created, if it does not already exist`s.
        ** END NOTE ***

        :param node_table_meta: dict - (pymedgraph.dataextraction.basepipe.NodeTable.meta) meta information about table,
         such as attribute columns and id
        :param node_label: str - Label of node, which will be created. E.g. "Gene" or "Paper" or "DISEASE"
        """
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
        node_string = 'MERGE (n: {n} {{{attr}}})'.format(n=node_label, attr=node_attr_string)
        # build final query
        query = 'UNWIND $rows AS row ' + node_string + ' RETURN count(*) as total'
        return query

    @staticmethod
    def _create_node_relation_query(node_table_meta: dict, node_label: str, source_node_label: str):
        """
        Build query to upload node relations to source node

        The nodes are selected for the relation linkage by their id attribute, which is stated in the `node_table_meta`
        dictionary.

        e.g.
            In this case the DISEASE node is the source node and UMLS is the current node which was uploaded before.

            DISEASE-[CONTAINS]->UMLS

            DISEASE ({text: phenylketonuria, ..}) -[:CONTAINS]-> UMLS ({CUI: C0031485, ..})

        :param node_table_meta: dict - (pymedgraph.dataextraction.basepipe.NodeTable.meta) meta information about table,
         such as attribute columns and id. This method uses `source_node_attr` & `source_column` to determine the
         relations.
        :param node_label: str - label of node, which was created
        :param source_node_label: str - label of source node, which is already in the database
         """
        match_query = 'MATCH (a: {A}), (b: {B})'.format(A=source_node_label, B=node_label)
        where_query = 'WHERE a.{a_attr} = row.{a_col} AND b.{b_attr_col} = row.{b_attr_col}'.format(
            a_attr=node_table_meta['source_node_attr'], a_col=node_table_meta['source_column'],
            b_attr_col=node_table_meta['id_attribute']
        )
        query = 'UNWIND $rows AS row ' + match_query + ' ' + where_query + \
                ' MERGE (a)-[:CONTAINS]->(b) RETURN count(*) as total'
        return query

    def insert_data(self, query: str, rows: pd.DataFrame, batch_size: int = 2000):
        """
        Wrapper method to call `Neo4jBuilder.query()` in batches to upload data to neo4j instance.

        :param query: str - query must contain UNWIND statement and
        :param rows: pd.DataFrame - contains either nodes + attributes or node relations
        :param batch_size: int - size of batch to be uploaded per session
        """
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
            if self.logger:
                self.logger.info('Successfully uploaded data: {r}'.format(r=result))
            print(result)

        return result

    def query(self, query, parameters):
        """
        Method to passes query and data to neo4j driver session.

        :param query: str
        :param parameters: dict
        """
        session = None
        response = None
        try:
            session = self.driver.session()
            response = list(session.run(query, parameters))
        except Exception as ex:
            if self.logger:
                self.logger.error('Query failed. qeury: \'{q}\'. {ex}'.format(q=query, ex=ex))
            print('Query failed:', ex)
        finally:
            if session:
                session.close()
        return response

    def _init_new_neo4j_graph(self, disease: str, delete_existing_graph=True):
        """ Method deletes graph and build new node for disease """
        if delete_existing_graph:
            delete_query = "MATCH (n) DETACH DELETE n"
            response = self.query(delete_query, None)
            if self.logger:
                self.logger.info('Successfully deleted existing graph.')
            print(response)
        init_query = "CREATE (st:SearchTerm {label: $disease})"  #TODO: MATCH
        response = self.query(init_query, {'disease': disease})
        print(response)
        if self.logger:
            self.logger.info(f'Successfully initiated graph with search term \'{disease}\'')

    def get_search_terms(self) -> list:
        """ Method to get existing Nodes with SearchTerm label in neo4j instance """
        query_string = "MATCH (s:SearchTerm) RETURN s"
        result = self.query(query_string, None)
        search_terms =  [r.get('s').get('label') for r in result]
        if self.logger:
            self.logger.info(f'Neo4j Request with query \'{query_string}\' and response: {search_terms}')
        return search_terms

    @staticmethod
    def get_node_data(node_table):
        return node_table.data.drop_duplicates(subset=[node_table.meta['id_attribute']])

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()
