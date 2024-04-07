import logging
import requests
import yaml
from yaml.loader import SafeLoader

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


DEFAULT_HOST = 'http://localhost'


class DataHubUtils:
    """
    """

    def_count: int = 5000
    graph: DataHubGraph
    session: requests.session

    def __init__(self):
        self._init_session()

    def _init_session(self):
        self.session = requests.session()
        self.graph = DataHubGraph(
            config=DatahubClientConfig(
                server=f"{DEFAULT_HOST}:8080",
            )
        )

    @staticmethod
    def load_yaml(path):
        with open(path, "r") as fp:
            r = yaml.load(fp, Loader=SafeLoader)
        return r

    @staticmethod
    def save_yaml(d, path):
        with open(path, "w") as fp:
            fp.write(yaml.dump(d))

    def get_entities(self, start=0, count=def_count, entity='dataset'):
        r = self.session.post(
            f"{DEFAULT_HOST}:8080/entities?action=listUrns",
            headers={
                'X-RestLi-Protocol-Version': '2.0.0',
                'Content-Type': 'application/json',
            },
            json={
                "entity": entity,
                "start": start,
                "count": count,
            },
        )
        assert r.status_code == 200, f"{r.status_code}: {str(r.text)}"
        rj = r.json()
        assert rj['value']['total'] < count
        return rj['value']['entities']

    def get_datasets_descriptions(self, start=0, count=def_count):
        qrd = '''
        query {
          search(input: {
            type: DATASET,
            query: "*",
            count: %count%,
            start: %start%,
          }) {
            total
            searchResults {
              entity {
                ... on Dataset {
                  urn
                  name
                  platform{
                    name
                  }
                  properties {
                    description
                  }
                  editableProperties {
                    description
                  }
                  schemaMetadata {
                    fields {
                      fieldPath
                      description
                    }
                  }
                  editableSchemaMetadata {
                    editableSchemaFieldInfo {
                      fieldPath
                      description
                    }
                  }
                }
              }
            }
          }
        }
        '''.replace('%start%', str(start)).replace('%count%', str(count))
        r = self.graph.execute_graphql(qrd)['search']
        assert r['total'] < count
        return [
            {
                'urn': dataset['entity']['urn'],
                'name': dataset['entity']['name'],
                'platform': dataset['entity']['platform']['name'],
                'description': dataset['entity']['editableProperties']['description'] if dataset['entity']['editableProperties'] else (
                    dataset['entity']['properties']['description'] if dataset['entity']['properties'] else None
                ),
                'fields': {
                    **({f['fieldPath']: f for f in dataset['entity']['schemaMetadata']['fields']} if dataset['entity']['schemaMetadata'] else {}),
                    **({f['fieldPath']: f for f in dataset['entity']['editableSchemaMetadata']['editableSchemaFieldInfo']} if dataset['entity']['editableSchemaMetadata'] else {}),
                }
            }
            for dataset in r['searchResults']
        ]

    def get_dataset_description(self, urn):
        qrd = '''
        query {
          dataset(urn: "%urn%") {
            properties {
              description
            }
            editableProperties {
              description
            }
          }
        }
        '''.replace('%urn%', urn)
        r = self.graph.execute_graphql(qrd)['dataset']
        return r['editableProperties']['description'] if r['editableProperties'] else (
            r['properties']['description'] if r['properties'] else None
        )

    def get_dataset_field_description(self, urn):
        qrd = '''
        query {
          dataset(urn: "%urn%") {
            schemaMetadata {
              fields {
                fieldPath
                description
              }
            }
            editableSchemaMetadata {
              editableSchemaFieldInfo {
                fieldPath
                description
              }
            }
          }
        }
        '''.replace('%urn%', urn)
        r = self.graph.execute_graphql(qrd)['dataset']
        return r['editableSchemaMetadata']['editableSchemaFieldInfo'] if r['editableSchemaMetadata'] else (
            r['schemaMetadata']['fields'] if r['schemaMetadata'] else None
        )

    def change_datasets_descriptions(self, datasets):
        for d in datasets:
            old_desc = self.get_dataset_description(d['urn'])
            if old_desc != d['description'] and d['description'] is not None:
                qcd = '''
                mutation updateDataset {
                  updateDataset(
                    urn:"%urn%",
                    input: {
                      editableProperties: {
                          description: """%description%"""
                      }
                    }
                  ) {
                    urn
                  }
                }
                '''.replace('%urn%', d['urn']).replace('%description%', d['description'])
                print(f"Rename '{old_desc}' in '{d['description']}'")
                self.graph.execute_graphql(qcd)

            old_field_desc = self.get_dataset_field_description(d['urn'])
            for fd in d['fields'].values():
                for ofd in old_field_desc:
                    if fd['fieldPath'] != ofd['fieldPath']:
                        continue
                    if fd['description'] != ofd['description'] and fd['description'] is not None:
                        qcd = '''
                        mutation updateDescription {
                          updateDescription(
                            input: {
                              description: """%description%""",
                              resourceUrn:"%urn%",
                              subResource: "%subResource%",
                              subResourceType:DATASET_FIELD
                            }
                          )
                        }
                        '''.replace('%urn%', d['urn']).replace('%description%', fd['description']).replace('%subResource%', fd['fieldPath'])
                        print(f"Rename '{ofd['description']}' in '{fd['description']}'")
                        self.graph.execute_graphql(qcd)
