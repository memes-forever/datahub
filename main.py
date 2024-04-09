import json
import yaml
import ruamel.yaml as ruamel

# from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn
# from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter, DataHubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# # Imports for metadata model classes
from datahub.metadata.schema_classes import (
    OwnershipClass,
    SchemaMetadataClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
)


def represent_none(self, _):
    return self.represent_scalar('tag:yaml.org,2002:null', '')


yaml.add_representer(type(None), represent_none)


class DataHubUtils:
    rest_emitter: DataHubRestEmitter
    graph: DataHubGraph
    gms_server = "http://localhost:8080"

    def __init__(self):
        pass

    def init(self):
        self.graph = DataHubGraph(DatahubClientConfig(server=self.gms_server))
        self.rest_emitter = DatahubRestEmitter(gms_server=self.gms_server)

    @staticmethod
    def order_dict_to_dict(d):
        return json.loads(json.dumps(d))

    def meta_classes_to_obj(self, d):
        return {k: self.order_dict_to_dict(v.to_obj()) for k, v in d.items() if v}

    @staticmethod
    def obj_to_meta_classes(d, meta_class):
        return {k: meta_class.from_obj(v) for k, v in d.items()}

    @staticmethod
    def load_yaml(config_file):
        print(f'Load obj from {config_file} ... ', end='')
        with open(config_file, 'r', encoding='utf-8') as files:
            results = yaml.load(files, ruamel.Loader)
        print('Done.')
        return results

    @staticmethod
    def save_yaml(config_file, obj):
        print(f'Save obj in {config_file} ... ', end='')
        with open(config_file, 'w', encoding='utf-8') as files:
            yaml.dump(obj, files, allow_unicode=True, sort_keys=False)
        print('Done.')

    def get_entities(self, urns, meta_class):
        results = {}
        for urn in urns:
            print(f'Get entity in {meta_class} ----> {urn}')
            results[urn] = self.graph.get_aspect(entity_urn=urn, aspect_type=meta_class)

        return results


if __name__ == "__main__":
    dhu = DataHubUtils()
    dhu.init()

    all_urns = list(dhu.graph.get_urns_by_filter())
    dhu.save_yaml('./configs/urns_dump.yaml', all_urns)

    entity_metaclass_list = [
        SchemaMetadataClass,
        DatasetPropertiesClass,
        OwnershipClass,
        GlobalTagsClass,
        # todo add other
    ]

    for m in entity_metaclass_list:
        all_urns_metadata = dhu.get_entities(all_urns, m)

        # save
        all_urns_metadata_obj = dhu.meta_classes_to_obj(all_urns_metadata)
        dhu.save_yaml(f'./configs/{m.ASPECT_NAME}.yaml', all_urns_metadata_obj)

        # restore
        # result_obj = dhu.load_yaml(f'./configs/{m.ASPECT_NAME}.yaml')
        # result = dhu.obj_to_meta_classes(result_obj, m)

        # todo after yaml to datahub sync

    a = 2
