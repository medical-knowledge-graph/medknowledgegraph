from neo4j import GraphDatabase


class Neo4jBuilder(object):
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def create_paper(self, paper):
        with self.driver.session() as session:
            for paper_uri, paper_dict in paper.items():
                result = session.write_transaction(
                    self._create_paper, paper_uri, paper_dict['title']
                )
                for row in result:
                    print(f'created {row}')

    @staticmethod
    def _create_paper(tx, paper_id, paper_title):
        query = (
            "CREATE (p:Paper {uri: $paper_id, title: $paper_title})"
            "RETURN p"
        )
        result = tx.run(query, paper_id=paper_id, paper_title=paper_title)
        return [{"p": row["p"]["uri"]} for row in result]
